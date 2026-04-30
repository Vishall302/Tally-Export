#!/usr/bin/env python3
"""
Pure-LLM expense blocklist filter for the TDS analysis pipeline.

Takes a list of ledger names (the output of ``list_expense_or_fixed_asset_ledgers.py``
or the in-memory set built by ``vouchers_liability_no_expense_yes.load_expense_and_liability_sets``)
and asks Claude to identify the ones that fall under any of the 11 PDF blocklist
categories (Discount, Round-off, Bad debts, P&L on sale of asset, Prior period,
Write-off, Bank charges, Late fees & penalties, GST, Income tax, ESI/PF).

Design choices (correctness-first):

- **No deterministic keyword stage.** Every name is judged by the LLM with the
  full PDF intents in the system prompt, so explicit nuances like "purchase-GST
  is not blocked", "Tax Audit Fees is not blocked", and "interest on loan to a
  vendor is not blocked (TDS u/s 194A)" are applied uniformly.
- **Forced tool use** with a strict JSON schema — the model cannot drift into
  free-form output.
- **Adaptive thinking** (Opus 4.7) — the model spends real reasoning on the
  ambiguous cases (cats 8/9/10).
- **Persistent JSON cache** keyed by lowercased name — re-runs are byte-identical
  and free.
- **Prompt caching** on the (large, frozen) system prompt — second batch onward
  pays a fraction of the input cost.
- **Default bias toward keep**: when the model is unsure it must set
  ``blocklisted=false``. Wrongly excluding a TDS-relevant ledger is far worse
  than including a blocklisted one.

CLI
---
  python apply_expense_blocklist.py \\
      --input expense_raw.json \\
      --config expense_blocklist_categories.json \\
      --output expense_filtered.json \\
      --report expense_blocklist_report.json

Library
-------
  from apply_expense_blocklist import filter_names, load_config

  config = load_config(Path("expense_blocklist_categories.json"))
  kept, report = filter_names(
      names=sorted(expense_or_fixed),
      config=config,
      cache_path=Path("expense_blocklist_cache.json"),
  )

Environment
-----------
  ANTHROPIC_API_KEY must be set.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

# Anthropic SDK is imported lazily so `--no-llm-check` style probes don't fail
# on machines that haven't installed it yet. The actual call site imports it.


# --------------------------------------------------------------------------- #
# Config + I/O                                                                #
# --------------------------------------------------------------------------- #

def load_config(config_path: Path) -> list[dict[str, Any]]:
    """Load and validate the blocklist categories JSON."""
    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)
    if not isinstance(config, list) or not config:
        raise ValueError(f"{config_path}: expected a non-empty JSON array")
    for entry in config:
        for required in ("id", "name", "intent"):
            if required not in entry:
                raise ValueError(f"{config_path}: category missing '{required}': {entry}")
    return config


def load_input_names(input_path: Path) -> list[str]:
    """Read names from a JSON array or a one-name-per-line text file."""
    text = input_path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text.startswith("["):
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError(f"{input_path}: JSON root must be an array of strings")
        return [str(x).strip() for x in data if str(x).strip()]
    # plain text — one name per line
    return [line.strip() for line in text.splitlines() if line.strip()]


def write_names(path: Path, names: Iterable[str], as_json: bool) -> None:
    sorted_names = sorted(set(names))
    with path.open("w", encoding="utf-8", newline="\n") as f:
        if as_json:
            json.dump(sorted_names, f, ensure_ascii=False, indent=2)
            f.write("\n")
        else:
            for n in sorted_names:
                f.write(n + "\n")


def write_report(path: Path, report: list[dict[str, Any]]) -> None:
    sorted_report = sorted(report, key=lambda r: r["name"].lower())
    with path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(sorted_report, f, ensure_ascii=False, indent=2)
        f.write("\n")


def load_cache(cache_path: Path) -> dict[str, dict[str, Any]]:
    if not cache_path.is_file():
        return {}
    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"WARNING: cache file unreadable ({exc}); starting empty.", file=sys.stderr)
        return {}


def save_cache(cache_path: Path, cache: dict[str, dict[str, Any]]) -> None:
    tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(cache_path)


# --------------------------------------------------------------------------- #
# Prompt + tool schema                                                        #
# --------------------------------------------------------------------------- #

def _build_tool_schema(include_reasons: bool) -> dict[str, Any]:
    """
    Build the record_decisions tool schema. When ``include_reasons=False``, the
    ``reason`` field is removed from the per-decision schema — this is the
    cheap-mode branch that cuts ~70% of output tokens.
    """
    item_properties: dict[str, Any] = {
        "name": {
            "type": "string",
            "description": "The ledger name verbatim, exactly as given in the input.",
        },
        "blocklisted": {
            "type": "boolean",
            "description": (
                "True if this ledger should be EXCLUDED from TDS analysis. "
                "Default to false when unsure — wrongly excluding a TDS-relevant ledger "
                "is worse than including a blocklisted one."
            ),
        },
        "category": {
            "type": ["integer", "null"],
            "description": "The category number 1-11 if blocklisted, otherwise null.",
        },
    }
    item_required = ["name", "blocklisted", "category"]
    if include_reasons:
        item_properties["reason"] = {
            "type": "string",
            "description": (
                "One-sentence justification citing the PDF category intent or stated "
                "nuance. For non-blocked names, briefly say why it stays (e.g. 'professional "
                "fees, TDS u/s 194J', 'rent payment', 'vendor expense')."
            ),
        }
        item_required.append("reason")

    return {
        "name": "record_decisions",
        "description": (
            "Record blocklist classification decisions for the batch of ledger names. "
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


def _thinking_config(model: str, no_thinking: bool) -> dict[str, str]:
    """
    Pick the right thinking config for the given model.

    - If the user explicitly disabled thinking, return ``{"type": "disabled"}``.
    - Haiku 4.5 and Sonnet 4.5 don't support adaptive thinking — they would 400.
      For these models we default to disabled.
    - Everything else (Opus 4.6/4.7, Sonnet 4.6) uses adaptive thinking.
    """
    if no_thinking:
        return {"type": "disabled"}
    m = model.lower()
    if "haiku" in m or "sonnet-4-5" in m:
        return {"type": "disabled"}
    return {"type": "adaptive"}


def _build_system_prompt(config: list[dict[str, Any]], include_reasons: bool = True) -> str:
    """Compose the (frozen) system prompt — the cacheable prefix."""
    cat_blocks = []
    for cat in config:
        kw_line = ""
        kws = cat.get("keywords") or []
        if kws:
            kw_line = "\n     Reference keywords (illustrative — judge by intent, not just keyword match): " + ", ".join(kws[:25])
            if len(kws) > 25:
                kw_line += f", ... ({len(kws) - 25} more)"
        cat_blocks.append(
            f"  Category {cat['id']} — {cat['name']}\n"
            f"     Intent: {cat['intent']}{kw_line}"
        )
    categories_text = "\n\n".join(cat_blocks)

    return (
        "You are an expert in Indian accounting (Tally) and Indian Income Tax (TDS) "
        "compliance. You are reviewing ledger names from a Tally daybook to decide which "
        "ones must be EXCLUDED from a TDS analysis.\n\n"
        "PIPELINE CONTEXT\n"
        "----------------\n"
        "A Tally daybook lists vouchers debiting ledgers classified as 'Expense' or 'Fixed "
        "Assets'. The downstream pipeline only cares about ledgers where a payment is being "
        "made to an external party (vendor, contractor, professional, landlord, employee, "
        "etc.) and TDS could conceivably apply. Several ledger types technically sit under "
        "'Expense' but are NOT TDS-relevant — these must be flagged as 'blocklisted'.\n\n"
        "THE 11 BLOCKLIST CATEGORIES\n"
        "---------------------------\n"
        f"{categories_text}\n\n"
        "CRITICAL NUANCES (these are explicit rules — apply them carefully)\n"
        "------------------------------------------------------------------\n"
        "1. GST nuance (Cat 9): A ledger named 'purchase-GST', 'Purchase IGST', "
        "'Sales CGST', or similar — where GST is part of a purchase/sales/asset ledger "
        "name — is NOT a Cat 9 blocklist. It's a transactional ledger that happens to have "
        "'GST' in the name. Only ledgers SOLELY representing a GST tax component owed to or "
        "paid to the government (ITC reversal, RCM payable, output GST, CGST/SGST/IGST/UTGST "
        "expense or payable in their own right) are Cat 9. If unsure, set blocklisted=false.\n\n"
        "2. Income Tax nuance (Cat 10): 'Tax Audit Fees', 'Tax Consultancy Fees', "
        "'Income Tax Audit Fees', 'Tax Filing Fees', 'TDS Return Filing Fees', and similar are "
        "NOT Cat 10 — they are professional fees paid to a CA/consultant and attract TDS u/s "
        "194J. Only ledgers SOLELY representing Income Tax payments to the government (advance "
        "tax, self-assessment tax, MAT, deferred tax, income-tax provision, TDS/TCS receivable, "
        "etc.) are Cat 10. The bare keyword 'tax' is not enough — judge by intent.\n\n"
        "3. Penalty / interest nuance (Cat 8): Statutory penalties and interest on statutory "
        "dues are blocked: GST late fees, ROC penalty, TDS interest, sec 234A/B/C interest, "
        "MCA late fees, ESIC/PF penalty, court fines, etc. But 'Interest paid to Vendor X on "
        "late payment', 'Penal interest on unsecured loan', 'Interest on car loan', or any "
        "interest paid to a non-government party is NOT Cat 8 — TDS u/s 194A applies. Block "
        "only when the payee is the government or a statutory authority.\n\n"
        "4. Write-off nuance (Cat 6): The PDF lists internal write-offs (stock, goods, fixed "
        "assets, goodwill, advances, prepaid, CWIP, etc.). These are accounting recognition "
        "with no payee. If a 'write off' name suggests a payment to an external party rather "
        "than an internal accounting entry, do not block — set blocklisted=false.\n\n"
        "DEFAULT BIAS\n"
        "------------\n"
        "If a name is ambiguous and you cannot confidently match an intent above, set "
        "blocklisted=false. Wrongly excluding a TDS-relevant ledger causes TDS non-compliance; "
        "wrongly including a blocklisted ledger only adds harmless noise downstream.\n\n"
        + (
            "OUTPUT\n"
            "------\n"
            "Use the record_decisions tool. Return one entry per input name in the SAME order, "
            "with the exact verbatim 'name' (do not normalize casing or strip punctuation). For "
            "every name include a one-sentence 'reason' — short, specific, and grounded in either "
            "a category intent or a TDS rule. Do not emit any text outside the tool call."
            if include_reasons
            else "OUTPUT\n"
            "------\n"
            "Use the record_decisions tool. Return one entry per input name in the SAME order, "
            "with the exact verbatim 'name' (do not normalize casing or strip punctuation). "
            "Return ONLY {name, blocklisted, category}. Do NOT include a 'reason' field — "
            "category-level reasoning will be derived from the category intent text. Be terse: "
            "no commentary, no preamble, no text outside the tool call."
        )
    )


# --------------------------------------------------------------------------- #
# LLM call                                                                    #
# --------------------------------------------------------------------------- #

def _classify_batch(
    client: Any,
    model: str,
    system_prompt: str,
    batch: list[str],
    max_tokens: int,
    tool_schema: dict[str, Any],
    thinking_cfg: dict[str, str],
) -> list[dict[str, Any]]:
    """One LLM call. Returns the raw decisions list (unsorted, unmerged)."""
    user_msg = (
        f"Classify these {len(batch)} ledger names. Return one decision per name in the "
        f"same order via the record_decisions tool.\n\n"
        + "\n".join(f"{i + 1}. {name}" for i, name in enumerate(batch))
    )

    # Use streaming so large max_tokens (with adaptive thinking) doesn't hit the SDK's
    # non-streaming guard. .get_final_message() gives us the assembled response.
    with client.messages.stream(
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
    ) as stream:
        message = stream.get_final_message()

    # Find the tool_use block. tool_choice forces it, so it must exist.
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


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #

def _synthesize_reason(
    blocklisted: bool,
    category: int | None,
    intent_by_id: dict[int, str],
    fallback: str = "",
) -> str:
    """Build a default reason from the category intent when the LLM didn't return one."""
    if blocklisted and category in intent_by_id:
        intent = intent_by_id[category].strip()
        # Trim to one sentence-ish chunk so the report stays readable.
        if len(intent) > 200:
            intent = intent[:197].rstrip(",;:- ") + "..."
        return f"PDF cat {category}: {intent}"
    if not blocklisted:
        return fallback or "Kept (no per-name reasoning requested)."
    return fallback or "(no reason returned)"


def filter_names(
    names: list[str],
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
) -> tuple[list[str], list[dict[str, Any]]]:
    """
    Run the pure-LLM blocklist filter.

    Parameters
    ----------
    names : list[str]
        Input ledger names. Order is preserved in the report; output is sorted.
    config : list[dict]
        Loaded ``expense_blocklist_categories.json``.
    cache_path : Path or None
        If given, reads/writes a persistent JSON cache keyed by lowercased name.
    model : str
        Anthropic model ID. Defaults to ``claude-haiku-4-5``.
    batch_size : int
        Names per LLM call.
    max_tokens_per_batch : int
        Output cap per batch.
    api_key : str or None
        Override ``ANTHROPIC_API_KEY`` env var.
    progress : bool
        Print progress to stderr.
    no_thinking : bool
        If True (or model is Haiku 4.5 / Sonnet 4.5), pass thinking=disabled.
        Cuts output cost dramatically.
    no_reasons : bool
        If True, drop ``reason`` from the LLM tool schema. The audit report
        synthesizes a category-level reason instead. Cuts output cost ~5×.
    concurrency : int
        Number of parallel LLM calls in flight at once. ``1`` = sequential
        (current behavior). Higher values reduce wall-clock time linearly
        until you hit Anthropic rate limits.

    Returns
    -------
    (kept_names, audit_report)
        ``kept_names`` is the input minus all names where ``blocklisted=True``
        (sorted, deduped). ``audit_report`` is a list of per-name records.
    """
    cache: dict[str, dict[str, Any]] = load_cache(cache_path) if cache_path else {}

    # Dedup but preserve first-seen order for stable reporting.
    seen: set[str] = set()
    unique_in_order: list[str] = []
    for n in names:
        key = n.lower()
        if key not in seen:
            seen.add(key)
            unique_in_order.append(n)

    # Partition into cache hits vs to-classify.
    todo: list[str] = []
    decisions_by_key: dict[str, dict[str, Any]] = {}
    for n in unique_in_order:
        key = n.lower()
        cached = cache.get(key)
        if cached is not None:
            # Use cached decision but keep the freshly-seen casing.
            decisions_by_key[key] = {**cached, "name": n, "source": "cache"}
        else:
            todo.append(n)

    if progress:
        print(
            f"Total: {len(unique_in_order)} | Cache hits: {len(decisions_by_key)} | "
            f"To classify: {len(todo)}",
            file=sys.stderr,
        )

    if todo:
        # Lazy import — only require the SDK when we'll actually call it.
        try:
            import anthropic  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "The 'anthropic' package is not installed. Run: pip install anthropic"
            ) from exc

        if api_key:
            client = anthropic.Anthropic(api_key=api_key)
        elif os.environ.get("ANTHROPIC_API_KEY"):
            client = anthropic.Anthropic()
        else:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not set. Set the env var or pass api_key=..."
            )

        # Pre-compute the things every batch needs (cheap, no API calls).
        include_reasons = not no_reasons
        system_prompt = _build_system_prompt(config, include_reasons=include_reasons)
        tool_schema = _build_tool_schema(include_reasons=include_reasons)
        thinking_cfg = _thinking_config(model, no_thinking)
        valid_categories = {cat["id"] for cat in config}
        intent_by_id = {cat["id"]: cat.get("intent", "") for cat in config}

        if progress:
            mode_bits = []
            if thinking_cfg["type"] == "disabled":
                mode_bits.append("thinking=off")
            else:
                mode_bits.append("thinking=adaptive")
            mode_bits.append("reasons=" + ("yes" if include_reasons else "no"))
            mode_bits.append(f"concurrency={concurrency}")
            mode_bits.append(f"batch={batch_size}")
            print(
                f"Mode: model={model} | " + " | ".join(mode_bits),
                file=sys.stderr,
            )

        # Slice todo into batches up-front so we can dispatch them to workers.
        batches: list[list[str]] = [
            todo[i : i + batch_size] for i in range(0, len(todo), batch_size)
        ]
        n_batches = len(batches)

        # Worker: one batch -> raw decisions list (or raises).
        def _run_one(batch: list[str]) -> list[dict[str, Any]]:
            try:
                return _classify_batch(
                    client, model, system_prompt, batch,
                    max_tokens_per_batch, tool_schema, thinking_cfg,
                )
            except Exception as exc:  # noqa: BLE001 — broad on purpose
                # One retry for transient API blips. With concurrency, don't
                # block other workers — sleep is short.
                time.sleep(2)
                return _classify_batch(
                    client, model, system_prompt, batch,
                    max_tokens_per_batch, tool_schema, thinking_cfg,
                )

        # Shared mutable state — protect with a lock when concurrency > 1.
        cache_lock = threading.Lock()
        completed_count = 0
        wall_t0 = time.monotonic()

        def _merge_batch_decisions(batch: list[str], decisions: list[dict[str, Any]]) -> None:
            """Index decisions by name, build records, update cache + decisions_by_key."""
            decisions_by_lc: dict[str, dict[str, Any]] = {}
            for d in decisions:
                if not isinstance(d, dict):
                    continue
                nm = str(d.get("name", "")).strip()
                if nm:
                    decisions_by_lc[nm.lower()] = d

            local_records: list[tuple[str, dict[str, Any]]] = []
            for original in batch:
                key = original.lower()
                d = decisions_by_lc.get(key)
                if d is None:
                    record = {
                        "name": original,
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
                            "name": original,
                            "blocklisted": False,
                            "category": None,
                            "reason": (
                                f"Model returned blocklisted=true with invalid category "
                                f"{cat!r}; defaulting to keep."
                            ),
                            "source": "llm-rejected",
                        }
                    else:
                        # Reason: use what the model returned if any, else synthesize.
                        raw_reason = str(d.get("reason", "") or "").strip()
                        reason = raw_reason or _synthesize_reason(
                            blocklisted, cat if blocklisted else None, intent_by_id
                        )
                        record = {
                            "name": original,
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
                        f"  Batch {i + 1}/{n_batches} ({len(batch)} names)...",
                        file=sys.stderr,
                        end=" ",
                        flush=True,
                    )
                t0 = time.monotonic()
                decisions = _run_one(batch)
                _merge_batch_decisions(batch, decisions)
                if progress:
                    print(f"done ({time.monotonic() - t0:.1f}s)", file=sys.stderr)
        else:
            # Parallel path.
            if progress:
                print(
                    f"  Dispatching {n_batches} batches with concurrency={concurrency}...",
                    file=sys.stderr,
                )
            with ThreadPoolExecutor(max_workers=concurrency) as ex:
                futures = {ex.submit(_run_one, b): b for b in batches}
                for fut in as_completed(futures):
                    batch = futures[fut]
                    try:
                        decisions = fut.result()
                    except Exception as exc:  # noqa: BLE001
                        # Final fallback: don't crash the whole run for one batch.
                        # Mark every name in the batch as default-keep.
                        if progress:
                            print(
                                f"\n    Batch failed permanently: {exc}. "
                                f"Defaulting {len(batch)} names to keep.",
                                file=sys.stderr,
                            )
                        decisions = []
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
    for n in unique_in_order:
        key = n.lower()
        rec = decisions_by_key.get(key)
        if rec is None:  # impossible, but defensive
            rec = {
                "name": n,
                "blocklisted": False,
                "category": None,
                "reason": "Internal: missing decision; defaulting to keep.",
                "source": "default-keep",
            }
        report.append(rec)
        if not rec["blocklisted"]:
            kept.append(n)

    return kept, report


# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #

def _summarize(report: list[dict[str, Any]]) -> str:
    blocked = [r for r in report if r["blocklisted"]]
    by_cat: dict[Any, int] = {}
    for r in blocked:
        by_cat[r["category"]] = by_cat.get(r["category"], 0) + 1
    cat_lines = "\n".join(
        f"    Category {cat}: {n}" for cat, n in sorted(by_cat.items(), key=lambda kv: kv[0] or 0)
    )
    sources: dict[str, int] = {}
    for r in report:
        sources[r["source"]] = sources.get(r["source"], 0) + 1
    src_lines = "\n".join(f"    {s}: {n}" for s, n in sorted(sources.items()))
    return (
        f"\nSummary\n-------\n"
        f"Total names: {len(report)}\n"
        f"Blocklisted: {len(blocked)}\n"
        f"Kept       : {len(report) - len(blocked)}\n"
        f"\n  Blocked by category:\n{cat_lines or '    (none)'}\n"
        f"\n  Decisions by source:\n{src_lines}\n"
    )


def main() -> None:
    base = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--input", type=Path, required=True,
        help="JSON array or one-name-per-line text file of ledger names.",
    )
    p.add_argument(
        "--config", type=Path, default=base / "expense_blocklist_categories.json",
        help="Categories config JSON (default: alongside this script).",
    )
    p.add_argument(
        "--output", type=Path, default=base / "expense_filtered.json",
        help="Where to write the filtered list (default: expense_filtered.json).",
    )
    p.add_argument(
        "--report", type=Path, default=base / "expense_blocklist_report.json",
        help="Per-name audit report JSON (default: expense_blocklist_report.json).",
    )
    p.add_argument(
        "--cache", type=Path, default=base / "expense_blocklist_cache.json",
        help="Persistent decision cache (default: expense_blocklist_cache.json).",
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
        help="Output token cap per batch (default: 32000). Streaming is used.",
    )
    p.add_argument(
        "--text", action="store_true",
        help="Write the filtered list as one name per line (default is JSON array).",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Write only the audit report; do not write the filtered list. "
             "Use this for the first review before committing.",
    )
    p.add_argument(
        "--no-thinking", action="store_true",
        help="Disable adaptive thinking (pass thinking={type:disabled}). Required "
             "for Haiku 4.5 / Sonnet 4.5 (which don't support adaptive thinking) and "
             "the single biggest output-cost saver in cheap mode.",
    )
    p.add_argument(
        "--no-reasons", action="store_true",
        help="Drop the per-name 'reason' field from the LLM output schema. The "
             "audit report still shows category + a synthesized category-level intent, "
             "but no per-name LLM reasoning. Cuts output cost ~5x — combine with "
             "--model claude-haiku-4-5 --no-thinking for ultra-cheap mode.",
    )
    p.add_argument(
        "--concurrency", type=int, default=1,
        help="Parallel LLM calls in flight at once (default: 1 = sequential). "
             "Higher values reduce wall-clock time roughly linearly until you hit "
             "Anthropic rate limits. Try 5 for cheap mode.",
    )
    args = p.parse_args()

    if not args.input.is_file():
        print(f"Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)
    if not args.config.is_file():
        print(f"Config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    config = load_config(args.config)
    names = load_input_names(args.input)
    if not names:
        print(f"Input is empty: {args.input}", file=sys.stderr)
        sys.exit(1)

    kept, report = filter_names(
        names=names,
        config=config,
        cache_path=args.cache,
        model=args.model,
        batch_size=args.batch_size,
        max_tokens_per_batch=args.max_tokens,
        no_thinking=args.no_thinking,
        no_reasons=args.no_reasons,
        concurrency=max(1, args.concurrency),
    )

    write_report(args.report, report)
    print(f"Audit report written to {args.report}", file=sys.stderr)

    if args.dry_run:
        print("--dry-run: skipping filtered list write.", file=sys.stderr)
    else:
        write_names(args.output, kept, as_json=not args.text)
        print(f"Filtered list ({len(kept)} names) written to {args.output}", file=sys.stderr)

    print(_summarize(report), file=sys.stderr)


if __name__ == "__main__":
    main()
