#!/usr/bin/env python3
"""
Pure-LLM party blocklist filter for the TDS analysis pipeline.

Takes the FINAL selected party list (liability/current-asset ledger names that
survived the voucher scan and group exclusion) and asks Claude to identify the
ones that are not real payees at all — statutory dues payable to government,
provision/accrual aggregates, tax-recoverable/prepaid adjustment ledgers, and
internal suspense/rounding accounts (4 categories in
``party_blocklist_categories.json``).

This is the party-side sibling of ``apply_expense_blocklist.py`` and reuses its
cache/report/thinking helpers. Differences that matter:

- **Each name is judged together with its parent group** from the ledger
  master — ``TDS Payable [group: Statutory Liabilities]`` is a much stronger
  signal than the bare name. The tool output still returns the bare name.
- **Cache key is (lowercased name, lowercased parent group)**, not name alone:
  the same ledger name can mean different things under different groups across
  clients, and the decision cache may be shared.
- **Employee protection is an explicit prompt rule**: employees' parent group
  is often literally "Salary Payable", which pattern-matches the provision
  category. A person-name ledger is NEVER blocklisted.
- **Default bias toward keep** (same asymmetry as the expense filter): wrongly
  excluding a real party causes TDS non-compliance; wrongly keeping a junk
  ledger only adds one noise row for the reviewer.

CLI
---
  python apply_party_blocklist.py \\
      --input final_candidates.json \\
      --ledgers tally_ledgers_final.xml \\
      --config party_blocklist_categories.json \\
      --output party_filtered.json \\
      --report party_blocklist_report.json

Library
-------
  from apply_party_blocklist import filter_parties, load_parent_groups

  parents = load_parent_groups(Path("tally_ledgers_final.xml"))
  kept, report = filter_parties(
      parties=[(n, parents.get(n, "")) for n in sorted(final_names)],
      config=load_config(Path("party_blocklist_categories.json")),
      cache_path=Path("party_blocklist_cache.json"),
  )

Environment
-----------
  ANTHROPIC_API_KEY, or licensed mode via TDS_PROXY_URL + TDS_LICENSE_TOKEN.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tds.apply_expense_blocklist import (  # noqa: E402
    _is_permanent_error,
    _synthesize_reason,
    _thinking_config,
    load_cache,
    load_config,
    load_input_names,
    save_cache,
    write_names,
    write_report,
)

log = logging.getLogger(__name__)


def load_parent_groups(ledgers_xml: Path) -> dict[str, str]:
    """Ledger display name -> direct PARENT group name, from the enriched master."""
    parents: dict[str, str] = {}
    for _event, elem in ET.iterparse(str(ledgers_xml), events=("end",)):
        if elem.tag != "LEDGER":
            continue
        name = (elem.get("NAME") or "").strip()
        if name:
            parents[name] = (elem.findtext("PARENT") or "").strip()
        elem.clear()
    return parents


def _cache_key(name: str, group: str) -> str:
    # Group is part of the key on purpose: "Advance" under "Loans & Advances
    # (Asset)" and "Advance" as a vendor trade name must not share a decision.
    return f"{name.lower()}||{group.lower()}"


def _build_tool_schema(include_reasons: bool) -> dict[str, Any]:
    """record_decisions schema; 'name' is the BARE ledger name (no group suffix)."""
    item_properties: dict[str, Any] = {
        "name": {
            "type": "string",
            "description": (
                "The ledger name verbatim, exactly as given before the [group: ...] "
                "annotation. Do NOT include the group annotation."
            ),
        },
        "blocklisted": {
            "type": "boolean",
            "description": (
                "True if this ledger is NOT a real payee and should be EXCLUDED from "
                "the TDS party list. Default to false when unsure — wrongly excluding "
                "a real party is worse than keeping a junk ledger."
            ),
        },
        "category": {
            "type": ["integer", "null"],
            "description": "The category number 1-4 if blocklisted, otherwise null.",
        },
    }
    item_required = ["name", "blocklisted", "category"]
    if include_reasons:
        item_properties["reason"] = {
            "type": "string",
            "description": (
                "One-sentence justification citing the category intent or a stated "
                "nuance. For kept names, briefly say why (e.g. 'vendor company', "
                "'employee ledger, section 192')."
            ),
        }
        item_required.append("reason")

    return {
        "name": "record_decisions",
        "description": (
            "Record party blocklist decisions for the batch of ledger names. "
            "Call this exactly once per batch with one entry per input name (preserve order)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "decisions": {
                    "type": "array",
                    "description": "One entry per ledger name in the user input.",
                    "items": {
                        "type": "object",
                        "properties": item_properties,
                        "required": item_required,
                    },
                }
            },
            "required": ["decisions"],
        },
    }


def _build_system_prompt(config: list[dict[str, Any]], include_reasons: bool = True) -> str:
    """Compose the (frozen) system prompt — the cacheable prefix."""
    cat_blocks = []
    for cat in config:
        kw_line = ""
        kws = cat.get("keywords") or []
        if kws:
            kw_line = (
                "\n     Reference keywords (illustrative — judge by intent, not just "
                "keyword match): " + ", ".join(kws[:25])
            )
            if len(kws) > 25:
                kw_line += f", ... ({len(kws) - 25} more)"
        cat_blocks.append(
            f"  Category {cat['id']} — {cat['name']}\n"
            f"     Intent: {cat['intent']}{kw_line}"
        )
    categories_text = "\n\n".join(cat_blocks)

    return (
        "You are an expert in Indian accounting (Tally) and Indian Income Tax (TDS) "
        "compliance. You are reviewing the FINAL selected party list of a TDS analysis: "
        "ledger names that were credited in expense vouchers and are therefore assumed "
        "to be payees (vendors, contractors, professionals, landlords, employees). Your "
        "job is to flag the ledgers that are NOT real payees at all.\n\n"
        "PIPELINE CONTEXT\n"
        "----------------\n"
        "Each input line is a ledger name followed by its Tally parent group in the "
        "form: name [group: parent group]. The parent group is a strong hint — e.g. a "
        "group like 'Statutory Liabilities' or 'GST Recoverable' corroborates a "
        "blocklist match, while 'Sundry Creditors' argues for a real vendor. Some "
        "companies use non-standard group names, so always judge name and group "
        "together by intent.\n\n"
        "THE 4 BLOCKLIST CATEGORIES\n"
        "--------------------------\n"
        f"{categories_text}\n\n"
        "CRITICAL NUANCES (these are explicit rules — apply them carefully)\n"
        "------------------------------------------------------------------\n"
        "1. Employee nuance: a ledger named after an individual PERSON (e.g. 'Ramesh "
        "Kumar', 'Jyoti Pandey', 'PRAVEEN SINGH BISHT', names with employee codes like "
        "'Nitin Sharma E.C. 132') is an EMPLOYEE ledger — a real payee whose salary "
        "attracts TDS u/s 192. Employees typically sit under a group literally named "
        "'Salary Payable' or similar. The GROUP name alone is NEVER a reason to block; "
        "NEVER blocklist a person-name ledger. Only an AGGREGATE bucket (a ledger "
        "itself named 'Salary Payable', 'Expenses Payable', 'Provision for ...') can "
        "be Category 2.\n\n"
        "2. Entity-name nuance: a business name that merely CONTAINS a category word "
        "is a real vendor — 'PF Consultants Pvt Ltd', 'GST Suvidha Kendra & Co', "
        "'Advance Decorative Laminates Pvt Ltd'. Entity markers (Pvt Ltd, LLP, & Co, "
        "& Associates, Enterprises, Traders, personal names) mean keep. Only ledgers "
        "that ARE the statutory due / provision / adjustment account itself are "
        "blocklisted.\n\n"
        "3. Spelling nuance: real Tally data contains typos and abbreviations — 'GST "
        "Payble RCM' is GST payable, 'Provisions for Exp' is a provision. Judge the "
        "intent behind the misspelled name, never require exact keyword spelling.\n\n"
        "4. Deposit/advance nuance: security deposits, earnest money, or vendor "
        "advance ledgers NAMED FOR A SPECIFIC PARTY (e.g. 'Security Deposit - Sharma "
        "Properties') still represent a real payee relationship — keep them. Only "
        "generic self-referential adjustment buckets are Category 3.\n\n"
        "DEFAULT BIAS\n"
        "------------\n"
        "If a name is ambiguous and you cannot confidently match an intent above, set "
        "blocklisted=false. Wrongly excluding a real party causes TDS non-compliance; "
        "wrongly keeping a junk ledger only adds one harmless row for the reviewer.\n\n"
        + (
            "OUTPUT\n"
            "------\n"
            "Use the record_decisions tool. Return one entry per input name in the SAME "
            "order. 'name' must be the BARE ledger name verbatim — exactly as given "
            "before the [group: ...] annotation, without the annotation, without "
            "normalizing casing or punctuation. For every name include a one-sentence "
            "'reason'. Do not emit any text outside the tool call."
            if include_reasons
            else "OUTPUT\n"
            "------\n"
            "Use the record_decisions tool. Return one entry per input name in the SAME "
            "order. 'name' must be the BARE ledger name verbatim — exactly as given "
            "before the [group: ...] annotation, without the annotation. Return ONLY "
            "{name, blocklisted, category}. Do NOT include a 'reason' field. Be terse: "
            "no commentary, no text outside the tool call."
        )
    )


def _make_client(api_key: str | None) -> Any:
    """Anthropic client: licensed proxy mode > explicit key > env key. Mirrors
    the resolution order in apply_expense_blocklist.filter_names."""
    try:
        import anthropic  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "The 'anthropic' package is not installed. Run: pip install anthropic"
        ) from exc

    proxy_url = os.environ.get("TDS_PROXY_URL", "").strip().rstrip("/")
    license_token = os.environ.get("TDS_LICENSE_TOKEN", "").strip()
    if proxy_url and license_token:
        return anthropic.Anthropic(api_key=license_token, base_url=f"{proxy_url}/anthropic")
    if api_key:
        return anthropic.Anthropic(api_key=api_key)
    if os.environ.get("ANTHROPIC_API_KEY"):
        return anthropic.Anthropic()
    raise RuntimeError(
        "No Anthropic access configured. Set ANTHROPIC_API_KEY, configure "
        "licensed mode (TDS_PROXY_URL + TDS_LICENSE_TOKEN), or pass api_key=..."
    )


def _classify_batch(
    client: Any,
    model: str,
    system_prompt: str,
    batch: list[tuple[str, str]],
    max_tokens: int,
    tool_schema: dict[str, Any],
    thinking_cfg: dict[str, str],
) -> list[dict[str, Any]]:
    """One LLM call over (name, parent_group) pairs. Returns raw decisions."""
    user_msg = (
        f"Classify these {len(batch)} party ledger names. Return one decision per name "
        f"in the same order via the record_decisions tool; 'name' must be the bare "
        f"ledger name without the [group: ...] annotation.\n\n"
        + "\n".join(
            f"{i + 1}. {name} [group: {group or '(unknown)'}]"
            for i, (name, group) in enumerate(batch)
        )
    )

    run_id = os.environ.get("TDS_RUN_ID", "").strip()
    extra = {"extra_headers": {"X-Run-Id": run_id}} if run_id else {}

    # Non-streaming on purpose — same rationale as the expense filter: one small
    # tool call, survives SSE-mangling proxies, explicit timeout.
    message = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        thinking=thinking_cfg,
        system=[
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        tools=[tool_schema],
        tool_choice={"type": "tool", "name": "record_decisions"},
        messages=[{"role": "user", "content": user_msg}],
        timeout=600.0,
        **extra,
    )

    tool_block = next(
        (b for b in message.content if getattr(b, "type", None) == "tool_use"),
        None,
    )
    if tool_block is None:
        snippet = next(
            (b.text for b in message.content if getattr(b, "type", None) == "text"),
            "(no text)",
        )
        raise RuntimeError(
            f"Model did not call record_decisions tool. stop_reason="
            f"{message.stop_reason!r}. Text snippet: {snippet[:300]}"
        )

    decisions = (tool_block.input or {}).get("decisions") or []
    if not isinstance(decisions, list):
        raise RuntimeError(f"Tool input 'decisions' is not a list: {decisions!r}")
    return decisions


def filter_parties(
    parties: list[tuple[str, str]],
    config: list[dict[str, Any]],
    cache_path: Path | None = None,
    model: str = "claude-haiku-4-5",
    batch_size: int = 25,
    max_tokens_per_batch: int = 32000,
    api_key: str | None = None,
    progress: bool = True,
    no_thinking: bool = False,
    no_reasons: bool = False,
    concurrency: int = 1,
    max_retries: int = 2,
    fail_on_llm_error: bool = True,
) -> tuple[list[str], list[dict[str, Any]]]:
    """
    Run the pure-LLM party blocklist filter over (name, parent_group) pairs.

    Returns ``(kept_names, audit_report)`` — kept_names preserves input order
    (deduped by cache key); the report has one record per unique input pair with
    ``name``, ``parent_group``, ``blocklisted``, ``category``, ``reason``,
    ``source``. Failed batches keep their names conservatively with
    ``source="error-keep"`` (never cached) when ``fail_on_llm_error=False``.

    ``concurrency`` is the number of parallel LLM calls in flight at once
    (default 1 = sequential, the old behavior). Real client party lists run to
    hundreds of names — 10+ batches — so the same thread-pool pattern as
    ``apply_expense_blocklist.filter_names`` applies here.
    """
    cache: dict[str, dict[str, Any]] = load_cache(cache_path) if cache_path else {}

    # Dedup by (name, group), preserving first-seen order.
    seen: set[str] = set()
    unique_in_order: list[tuple[str, str]] = []
    for name, group in parties:
        key = _cache_key(name, group)
        if key not in seen:
            seen.add(key)
            unique_in_order.append((name, group))

    todo: list[tuple[str, str]] = []
    decisions_by_key: dict[str, dict[str, Any]] = {}
    for name, group in unique_in_order:
        key = _cache_key(name, group)
        cached = cache.get(key)
        if cached is not None:
            decisions_by_key[key] = {**cached, "name": name, "source": "cache"}
        else:
            todo.append((name, group))

    if progress:
        print(
            f"Party filter — total: {len(unique_in_order)} | cache hits: "
            f"{len(decisions_by_key)} | to classify: {len(todo)}",
            file=sys.stderr,
        )

    if todo:
        client = _make_client(api_key)
        include_reasons = not no_reasons
        system_prompt = _build_system_prompt(config, include_reasons=include_reasons)
        tool_schema = _build_tool_schema(include_reasons=include_reasons)
        thinking_cfg = _thinking_config(model, no_thinking)
        valid_categories = {cat["id"] for cat in config}
        intent_by_id = {cat["id"]: cat.get("intent", "") for cat in config}

        batches = [todo[i : i + batch_size] for i in range(0, len(todo), batch_size)]
        n_batches = len(batches)

        # Shared mutable state — protect with a lock when concurrency > 1.
        cache_lock = threading.Lock()
        completed_count = 0
        wall_t0 = time.monotonic()
        # First permanent licence error seen (401/402/403). Once set, every
        # remaining batch fails instantly instead of hammering the proxy.
        hard_fail: dict[str, Exception | None] = {"exc": None}

        # Worker: one batch -> raw decisions list (or raises).
        def _run_one(batch: list[tuple[str, str]]) -> list[dict[str, Any]]:
            if hard_fail["exc"] is not None:
                raise hard_fail["exc"]
            last_exc: Exception | None = None
            for attempt in range(1 + max_retries):
                try:
                    return _classify_batch(
                        client, model, system_prompt, batch,
                        max_tokens_per_batch, tool_schema, thinking_cfg,
                    )
                except Exception as exc:  # noqa: BLE001 — broad on purpose
                    last_exc = exc
                    if _is_permanent_error(exc):
                        hard_fail["exc"] = exc
                        break
                    if attempt < max_retries:
                        time.sleep(2 * (2 ** attempt))
            assert last_exc is not None
            raise last_exc

        def _merge_error_keep(batch: list[tuple[str, str]], exc: Exception) -> None:
            """Record a failed batch as conservative keeps. Never cached, so a
            re-run after the problem is fixed re-classifies these names."""
            log.warning(
                "Party blocklist batch failed permanently (%d names kept): %s",
                len(batch), exc,
            )
            reason = (
                f"AI classification unavailable ({exc}); kept conservatively "
                "(not cached)."
            )
            with cache_lock:
                for name, group in batch:
                    decisions_by_key[_cache_key(name, group)] = {
                        "name": name,
                        "parent_group": group,
                        "blocklisted": False,
                        "category": None,
                        "reason": reason,
                        "source": "error-keep",
                    }

        def _merge_batch_decisions(
            batch: list[tuple[str, str]], decisions: list[dict[str, Any]]
        ) -> None:
            """Index decisions by name, build records, update cache + decisions_by_key."""
            # Index decisions by lowercased bare name; tolerate a model that
            # echoed the "[group: ...]" annotation despite instructions.
            decisions_by_lc: dict[str, dict[str, Any]] = {}
            for d in decisions:
                if not isinstance(d, dict):
                    continue
                nm = str(d.get("name", "")).strip()
                if "[group:" in nm:
                    nm = nm.split("[group:", 1)[0].strip()
                if nm:
                    decisions_by_lc[nm.lower()] = d

            local_records: list[tuple[str, dict[str, Any]]] = []
            for name, group in batch:
                key = _cache_key(name, group)
                d = decisions_by_lc.get(name.lower())
                if d is None:
                    record = {
                        "name": name,
                        "parent_group": group,
                        "blocklisted": False,
                        "category": None,
                        "reason": "Model omitted this name from its decisions; defaulting to keep.",
                        "source": "default-keep",
                    }
                else:
                    blocklisted = bool(d.get("blocklisted"))
                    cat = d.get("category")
                    if blocklisted and cat not in valid_categories:
                        record = {
                            "name": name,
                            "parent_group": group,
                            "blocklisted": False,
                            "category": None,
                            "reason": (
                                f"Model returned blocklisted=true with invalid category "
                                f"{cat!r}; defaulting to keep."
                            ),
                            "source": "llm-rejected",
                        }
                    else:
                        raw_reason = str(d.get("reason", "") or "").strip()
                        reason = raw_reason or _synthesize_reason(
                            blocklisted, cat if blocklisted else None, intent_by_id
                        )
                        record = {
                            "name": name,
                            "parent_group": group,
                            "blocklisted": blocklisted,
                            "category": cat if blocklisted else None,
                            "reason": reason,
                            "source": "llm",
                        }
                local_records.append((key, record))

            # Single critical section — fast.
            with cache_lock:
                for key, record in local_records:
                    decisions_by_key[key] = record
                    if cache_path is not None:
                        cache[key] = {
                            "name": record["name"],
                            "parent_group": record["parent_group"],
                            "blocklisted": record["blocklisted"],
                            "category": record["category"],
                            "reason": record["reason"],
                        }
                if cache_path is not None:
                    save_cache(cache_path, cache)

        # Sequential path (concurrency=1) — preserves old logging behavior.
        if concurrency <= 1:
            for i, batch in enumerate(batches):
                if progress:
                    print(
                        f"  Party batch {i + 1}/{n_batches} ({len(batch)} names)...",
                        file=sys.stderr,
                        end=" ",
                        flush=True,
                    )
                t0 = time.monotonic()
                try:
                    decisions = _run_one(batch)
                except Exception as exc:  # noqa: BLE001
                    if fail_on_llm_error:
                        raise
                    if progress:
                        print(f"failed: {exc}. Keeping {len(batch)} names.", file=sys.stderr)
                    _merge_error_keep(batch, exc)
                    continue
                _merge_batch_decisions(batch, decisions)
                if progress:
                    print(f"done ({time.monotonic() - t0:.1f}s)", file=sys.stderr)
        else:
            # Parallel path.
            if progress:
                print(
                    f"  Dispatching {n_batches} party batches with "
                    f"concurrency={concurrency}...",
                    file=sys.stderr,
                )
            with ThreadPoolExecutor(max_workers=concurrency) as ex:
                futures = {ex.submit(_run_one, b): b for b in batches}
                for fut in as_completed(futures):
                    batch = futures[fut]
                    try:
                        decisions = fut.result()
                    except Exception as exc:  # noqa: BLE001
                        if fail_on_llm_error:
                            # In-flight batches finish fast: hard_fail (if set)
                            # makes the remaining workers raise immediately.
                            raise
                        if progress:
                            print(
                                f"\n    Party batch failed permanently: {exc}. "
                                f"Keeping {len(batch)} names.",
                                file=sys.stderr,
                            )
                        _merge_error_keep(batch, exc)
                        completed_count += 1
                        continue
                    _merge_batch_decisions(batch, decisions)
                    completed_count += 1
                    if progress:
                        elapsed = time.monotonic() - wall_t0
                        print(
                            f"  [{completed_count}/{n_batches}] done "
                            f"(wall {elapsed:.1f}s)",
                            file=sys.stderr,
                        )

    # Build outputs preserving input order.
    report: list[dict[str, Any]] = []
    kept: list[str] = []
    for name, group in unique_in_order:
        key = _cache_key(name, group)
        rec = decisions_by_key.get(key)
        if rec is None:  # impossible, but defensive
            rec = {
                "name": name,
                "parent_group": group,
                "blocklisted": False,
                "category": None,
                "reason": "Internal: missing decision; defaulting to keep.",
                "source": "default-keep",
            }
        rec.setdefault("parent_group", group)
        report.append(rec)
        if not rec["blocklisted"]:
            kept.append(name)

    return kept, report


def summarize_party_llm_failures(report: list[dict[str, Any]]) -> dict[str, Any]:
    """Count ``error-keep`` records so callers can surface a partial-failure warning."""
    failed = [r for r in report if r.get("source") == "error-keep"]
    return {
        "failed_names": len(failed),
        "total_names": len(report),
        "reason": failed[0]["reason"] if failed else "",
    }


def main() -> None:
    base = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--input", type=Path, required=True,
        help="JSON array or one-name-per-line text file of selected party names.",
    )
    p.add_argument(
        "--ledgers", type=Path, required=True,
        help="Enriched TALLYLEDGERS XML — source of each party's parent group.",
    )
    p.add_argument(
        "--config", type=Path,
        default=base.parent / "config" / "party_blocklist_categories.json",
        help="Categories config JSON (default: config/party_blocklist_categories.json).",
    )
    p.add_argument(
        "--output", type=Path, default=base / "party_filtered.json",
        help="Where to write the filtered party list (default: party_filtered.json).",
    )
    p.add_argument(
        "--report", type=Path, default=base / "party_blocklist_report.json",
        help="Per-name audit report JSON (default: party_blocklist_report.json).",
    )
    p.add_argument(
        "--cache", type=Path, default=base / "party_blocklist_cache.json",
        help="Persistent decision cache (default: party_blocklist_cache.json).",
    )
    p.add_argument(
        "--model", default="claude-haiku-4-5",
        help="Anthropic model ID (default: claude-haiku-4-5).",
    )
    p.add_argument(
        "--batch-size", type=int, default=25,
        help="Names per LLM call (default: 25).",
    )
    p.add_argument(
        "--max-tokens", type=int, default=32000,
        help="Output token cap per batch (default: 32000).",
    )
    p.add_argument(
        "--text", action="store_true",
        help="Write the filtered list as one name per line (default is JSON array).",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Write only the audit report; do not write the filtered list.",
    )
    p.add_argument(
        "--no-thinking", action="store_true",
        help="Disable adaptive thinking (required for Haiku 4.5 / Sonnet 4.5).",
    )
    p.add_argument(
        "--no-reasons", action="store_true",
        help="Drop per-name 'reason' from the LLM output schema (cheap mode).",
    )
    p.add_argument(
        "--concurrency", type=int, default=1,
        help="Parallel LLM calls in flight at once (default: 1 = sequential).",
    )
    p.add_argument(
        "--keep-on-llm-error", action="store_true",
        help="Do not abort when a batch fails after retries: keep its names "
             "(source=error-keep, never cached) and continue.",
    )
    args = p.parse_args()

    if not args.input.is_file():
        print(f"Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)
    if not args.ledgers.is_file():
        print(f"Ledgers file not found: {args.ledgers}", file=sys.stderr)
        sys.exit(1)
    if not args.config.is_file():
        print(f"Config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    config = load_config(args.config)
    names = load_input_names(args.input)
    if not names:
        print(f"Input is empty: {args.input}", file=sys.stderr)
        sys.exit(1)

    parents = load_parent_groups(args.ledgers)
    kept, report = filter_parties(
        parties=[(n, parents.get(n, "")) for n in names],
        config=config,
        cache_path=args.cache,
        model=args.model,
        batch_size=args.batch_size,
        max_tokens_per_batch=args.max_tokens,
        no_thinking=args.no_thinking,
        no_reasons=args.no_reasons,
        concurrency=max(1, args.concurrency),
        fail_on_llm_error=not args.keep_on_llm_error,
    )

    write_report(args.report, report)
    print(f"Audit report written to {args.report}", file=sys.stderr)

    if args.dry_run:
        print("--dry-run: skipping filtered list write.", file=sys.stderr)
    else:
        write_names(args.output, kept, as_json=not args.text)
        print(f"Filtered party list ({len(kept)} names) written to {args.output}", file=sys.stderr)

    blocked = [r for r in report if r["blocklisted"]]
    print(
        f"\nSummary\n-------\nTotal: {len(report)} | Blocklisted: {len(blocked)} | "
        f"Kept: {len(report) - len(blocked)}",
        file=sys.stderr,
    )
    for r in blocked:
        print(f"  cat{r['category']}  {r['name']}  [{r.get('parent_group','')}]", file=sys.stderr)


if __name__ == "__main__":
    main()
