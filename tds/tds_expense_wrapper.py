#!/usr/bin/env python3
"""
End-to-end TDS-analysis pipeline wrapper.

Drives the same flow as ``analyze/final_list.py`` but inserts the pure-LLM blocklist
filter from ``apply_expense_blocklist.py`` between ledger-classification and
voucher-scanning, then applies the same ``core.groups`` group-tree exclusion
``analyze/final_list.py`` uses. **No existing script is modified.** The
wrapper imports public functions from:

  - ``core.ledger_sets`` — ``load_expense_and_liability_sets``
  - ``analyze.detect_cross_vouchers`` — ``collect_matching_liability_names``
  - ``core.groups`` — ``parent_names_from_roots``,
    ``ledgers_with_parent_in``, ``DEFAULT_ROOT_GROUPS``
  - ``tds.apply_expense_blocklist`` — ``filter_names``, ``load_config``

(This is the same import pattern ``final_list.py`` already uses.)

Pipeline
--------
1. Read enriched ledgers XML → build raw expense_or_fixed and liability_or_current sets.
2. Call ``apply_expense_blocklist.filter_names`` (pure LLM) on the raw expense set.
3. Call ``collect_matching_liability_names`` with the FILTERED expense set + the
   unchanged liability set against the daybook XML.
4. Subtract group-excluded ledgers (Duties & Taxes / Cash-in-Hand / Bank Accounts /
   Branch / Divisions) — same as ``final_list.py``. Skip with ``--no-group-exclusion``.
5. Write the sorted result to ``--output`` and the per-ledger audit report to
   ``--report``. Always writes the filtered expense set to ``--filtered-expense``
   for transparency.

The ``--output`` file (``final.txt`` in the pipeline) is the SINGLE SOURCE OF
TRUTH for the final ledger list. Downstream steps must consume it — e.g.
``output/split_by_ledger.py --final-names final.txt`` — and must NOT re-derive via
``analyze/final_list.py``, which implements only stages 1-4 and would drop the
materiality floor + party blocklist results.

Usage
-----
  python tds_expense_wrapper.py

  # Subsequent runs are byte-identical and free thanks to the cache.

Environment
-----------
  ANTHROPIC_API_KEY must be set.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tds.apply_expense_blocklist import (  # noqa: E402
    filter_names,
    load_config,
    summarize_llm_failures,
    write_names,
    write_report,
)
from tds.apply_party_blocklist import (  # noqa: E402
    filter_parties,
    load_parent_groups,
    summarize_party_llm_failures,
)
from core.groups import (  # noqa: E402
    DEFAULT_ROOT_GROUPS,
    ledgers_with_parent_in,
    parent_names_from_roots,
)
from core.ledger_sets import load_expense_and_liability_sets  # noqa: E402
from analyze.detect_cross_vouchers import collect_matching_liability_amounts  # noqa: E402


def run_tds_selection(
    *,
    ledgers: Path,
    daybook: Path,
    groups_xml: Path,
    config: Path,
    output: Path,
    report: Path,
    filtered_expense: Path,
    cache: Path,
    model: str = "claude-haiku-4-5",
    batch_size: int = 25,
    max_tokens: int = 32000,
    no_thinking: bool = False,
    no_reasons: bool = False,
    concurrency: int = 1,
    as_json: bool = False,
    no_group_exclusion: bool = False,
    fail_on_llm_error: bool = True,
    min_party_amount: float = 100.0,
    party_config: Path | None = None,
    party_report: Path | None = None,
    party_cache: Path | None = None,
    no_party_filter: bool = False,
    party_dry_run: bool = False,
    progress_cb=None,
) -> list[str]:
    """Run the TDS ledger selection (LLM expense blocklist + voucher scan +
    group exclusion + materiality floor + LLM party blocklist) and write the
    sorted result to *output*.

    Returns the sorted list of final ledger names. This is the importable core
    that both ``main()`` (CLI) and the webapp pipeline call. ``progress_cb`` — if
    given — is invoked as ``progress_cb(stage: str, detail: str)`` at each stage.
    Requires ``ANTHROPIC_API_KEY`` in the environment (LLM filter is always on).

    False-positive controls (Stages 4.5 and 5):
      - ``min_party_amount`` — drop parties whose TOTAL ``abs(AMOUNT)`` credited
        across all matching vouchers is below this floor (default ₹100; 0
        disables). Kills paise-level rounding-entry parties.
      - Party blocklist — LLM pass over the selected names (with parent groups)
        that removes statutory-payable / provision / prepaid / suspense ledgers.
        ``no_party_filter=True`` skips it; ``party_dry_run=True`` writes the
        audit report but does not remove anything. Defaults for the artifact
        paths sit next to *output*.
    """
    def _emit(stage: str, detail: str = "") -> None:
        if progress_cb is not None:
            progress_cb(stage, detail)

    # Default party-filter artifact paths sit next to the final output.
    _base_cfg = Path(__file__).resolve().parent.parent / "config"
    if party_config is None:
        party_config = _base_cfg / "party_blocklist_categories.json"
    if party_report is None:
        party_report = output.parent / "party_blocklist_report.json"
    if party_cache is None:
        party_cache = output.parent / "party_blocklist_cache.json"

    # Validate inputs.
    if not ledgers.is_file():
        raise FileNotFoundError(f"Ledgers file not found: {ledgers}")
    if not config.is_file():
        raise FileNotFoundError(f"Config file not found: {config}")
    if not daybook.is_file():
        raise FileNotFoundError(f"Daybook file not found: {daybook}")
    if not no_group_exclusion and not groups_xml.is_file():
        raise FileNotFoundError(f"Groups file not found: {groups_xml}")
    if not no_party_filter and not party_config.is_file():
        raise FileNotFoundError(f"Party blocklist config not found: {party_config}")

    # ----- Stage 1: build the raw sets from the ledgers XML -----
    _emit("classify", "Classifying ledgers from XML")
    print("Stage 1: classifying ledgers from XML (auto-detect off)...", file=sys.stderr)
    expense_or_fixed, liability_or_current = load_expense_and_liability_sets(
        ledgers, auto_detect=False
    )
    print(
        f"  expense_or_fixed   : {len(expense_or_fixed)} names\n"
        f"  liability_or_current: {len(liability_or_current)} names",
        file=sys.stderr,
    )

    # ----- Stage 2: pure-LLM blocklist filter on the expense set -----
    _emit("llm_filter", "LLM expense blocklist filter")
    print("\nStage 2: LLM blocklist filter (this can take a minute on first run)...",
          file=sys.stderr)
    cfg = load_config(config)
    raw_names = sorted(expense_or_fixed)
    kept_names, report_data = filter_names(
        names=raw_names,
        config=cfg,
        cache_path=cache,
        model=model,
        batch_size=batch_size,
        max_tokens_per_batch=max_tokens,
        no_thinking=no_thinking,
        no_reasons=no_reasons,
        concurrency=max(1, concurrency),
        fail_on_llm_error=fail_on_llm_error,
    )
    filtered_expense_set = set(kept_names)

    llm_fail = summarize_llm_failures(report_data)
    if llm_fail["failed_names"]:
        _emit(
            "warning",
            f"AI expense filter partly failed — {llm_fail['failed_names']} of "
            f"{llm_fail['total_names']} ledger names could not be AI-reviewed "
            f"and were kept in the audit. Reason: {llm_fail['reason']}",
        )

    # Always write the audit artifacts so the user can review them.
    write_report(report, report_data)
    write_names(filtered_expense, kept_names, as_json=True)
    blocked = len(raw_names) - len(kept_names)
    print(
        f"\n  Blocklisted: {blocked} | Kept: {len(kept_names)} / {len(raw_names)}\n"
        f"  Audit report   : {report}\n"
        f"  Filtered set   : {filtered_expense}",
        file=sys.stderr,
    )

    # ----- Stage 3: voucher scan with the filtered expense set -----
    _emit("voucher_scan", "Scanning daybook vouchers")
    print("\nStage 3: scanning daybook vouchers with filtered expense set...",
          file=sys.stderr)
    matching_amounts = collect_matching_liability_amounts(
        daybook, filtered_expense_set, liability_or_current
    )
    matching = set(matching_amounts)
    print(f"  voucher-pattern ledger names: {len(matching)}", file=sys.stderr)

    # ----- Stage 4: group-tree exclusion (same as final_list.py) -----
    if no_group_exclusion:
        print("\nStage 4: SKIPPED (no_group_exclusion).", file=sys.stderr)
        final_set = matching
    else:
        _emit("group_exclusion", "Excluding duties/cash/bank/branch groups")
        print(
            "\nStage 4: subtracting ledgers under duties/cash/bank/branch groups...",
            file=sys.stderr,
        )
        parent_names, _missing_roots = parent_names_from_roots(
            str(groups_xml), list(DEFAULT_ROOT_GROUPS)
        )
        excluded = set(ledgers_with_parent_in(str(ledgers), parent_names))
        print(
            f"  group-excluded ledger names: {len(excluded)}\n"
            f"  intersection (removed)     : {len(matching & excluded)}",
            file=sys.stderr,
        )
        final_set = matching - excluded

    # ----- Stage 4.5: materiality floor (rounding-entry parties) -----
    party_audit: list[dict] = []
    if min_party_amount > 0:
        _emit("materiality_floor", f"Dropping parties credited under ₹{min_party_amount:g}")
        floored = {n for n in final_set if matching_amounts.get(n, 0.0) < min_party_amount}
        if floored:
            print(
                f"\nStage 4.5: materiality floor ₹{min_party_amount:g} — dropping "
                f"{len(floored)} name(s):",
                file=sys.stderr,
            )
            for n in sorted(floored):
                amt = matching_amounts.get(n, 0.0)
                print(f"  ₹{amt:,.2f}  {n}", file=sys.stderr)
                party_audit.append({
                    "name": n,
                    "parent_group": "",
                    "blocklisted": True,
                    "category": None,
                    "reason": (
                        f"Total credited in matching vouchers ₹{amt:,.2f} is below the "
                        f"materiality floor ₹{min_party_amount:g} (rounding-entry noise)."
                    ),
                    "source": "materiality-floor",
                })
            final_set = final_set - floored
        else:
            print(
                f"\nStage 4.5: materiality floor ₹{min_party_amount:g} — nothing to drop.",
                file=sys.stderr,
            )
    else:
        print("\nStage 4.5: SKIPPED (min_party_amount=0).", file=sys.stderr)

    # ----- Stage 5: LLM party blocklist (statutory/provision/prepaid/suspense) -----
    if no_party_filter:
        print("\nStage 5: SKIPPED (no_party_filter).", file=sys.stderr)
    elif final_set:
        _emit("party_filter", "AI check: removing non-party ledgers")
        print("\nStage 5: LLM party blocklist over the selected names...", file=sys.stderr)
        parents = load_parent_groups(ledgers)
        party_cfg = load_config(party_config)
        kept_parties, party_llm_report = filter_parties(
            parties=[(n, parents.get(n, "")) for n in sorted(final_set)],
            config=party_cfg,
            cache_path=party_cache,
            model=model,
            batch_size=batch_size,
            max_tokens_per_batch=max_tokens,
            no_thinking=no_thinking,
            no_reasons=no_reasons,
            concurrency=max(1, concurrency),
            fail_on_llm_error=fail_on_llm_error,
        )
        party_audit.extend(party_llm_report)

        party_fail = summarize_party_llm_failures(party_llm_report)
        if party_fail["failed_names"]:
            _emit(
                "warning",
                f"AI party filter partly failed — {party_fail['failed_names']} of "
                f"{party_fail['total_names']} party names could not be AI-reviewed "
                f"and were kept. Reason: {party_fail['reason']}",
            )

        blocked_parties = sorted(final_set - set(kept_parties))
        if party_dry_run:
            print(
                f"  DRY-RUN: {len(blocked_parties)} name(s) flagged but NOT removed:",
                file=sys.stderr,
            )
            for n in blocked_parties:
                print(f"    {n}", file=sys.stderr)
        else:
            if blocked_parties:
                print(f"  Removed {len(blocked_parties)} non-party ledger(s):", file=sys.stderr)
                for n in blocked_parties:
                    print(f"    {n}", file=sys.stderr)
            else:
                print("  No non-party ledgers found.", file=sys.stderr)
            final_set = set(kept_parties)

    # Always write the party audit so every removal (floor + LLM) is reviewable.
    write_report(party_report, party_audit)
    print(f"  Party audit report: {party_report}", file=sys.stderr)

    sorted_names = sorted(final_set)
    print(f"\nFinal ledger names: {len(sorted_names)}", file=sys.stderr)

    # ----- Write final output -----
    with output.open("w", encoding="utf-8", newline="\n") as f:
        if as_json:
            json.dump(sorted_names, f, ensure_ascii=False, indent=2)
            f.write("\n")
        else:
            for n in sorted_names:
                f.write(n + "\n")
    print(f"\nFinal output written to {output}", file=sys.stderr)
    _emit("selection_done", f"{len(sorted_names)} ledgers selected")
    return sorted_names


def main() -> None:
    base = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--ledgers", type=Path, default=base / "tally_ledgers_final.xml",
        help="Enriched TALLYLEDGERS XML (default: tally_ledgers_final.xml).",
    )
    p.add_argument(
        "--daybook", type=Path, default=base / "daybook_01042024_to_31032025.xml",
        help="TALLYDAYBOOK XML (default: daybook_01042024_to_31032025.xml).",
    )
    p.add_argument(
        "--groups-xml", type=Path, default=base / "tally_groups_final.xml",
        help="Groups XML for the duties/cash/bank exclusion closure "
             "(default: tally_groups_final.xml). Used unless --no-group-exclusion.",
    )
    p.add_argument(
        "--config", type=Path, default=base / "expense_blocklist_categories.json",
        help="Blocklist categories config JSON.",
    )
    p.add_argument(
        "--output", type=Path, default=base / "test_filtered.txt",
        help="Final voucher-scan output (sorted liability/current-asset names).",
    )
    p.add_argument(
        "--report", type=Path, default=base / "expense_blocklist_report.json",
        help="Per-ledger LLM audit report.",
    )
    p.add_argument(
        "--filtered-expense", type=Path, default=base / "expense_filtered.json",
        help="Intermediate: the expense set after blocklist filtering. "
             "Useful for diffing against the raw set.",
    )
    p.add_argument(
        "--cache", type=Path, default=base / "expense_blocklist_cache.json",
        help="Persistent LLM decision cache.",
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
        help="Output token cap per LLM batch (default: 32000).",
    )
    p.add_argument(
        "--json", action="store_true",
        help="Write --output as a sorted JSON array (default: one name per line).",
    )
    p.add_argument(
        "--no-group-exclusion", action="store_true",
        help="Skip the duties/cash/bank/branch group exclusion (Stage 4). "
             "Useful for diagnostic runs that want the raw blocklist+voucher result.",
    )
    p.add_argument(
        "--no-thinking", action="store_true",
        help="Disable adaptive thinking in the LLM filter (passed through to "
             "apply_expense_blocklist). Required for Haiku 4.5.",
    )
    p.add_argument(
        "--no-reasons", action="store_true",
        help="Drop per-name 'reason' from the LLM output schema. Cuts cost ~5x; "
             "audit report falls back to category-level intent text.",
    )
    p.add_argument(
        "--concurrency", type=int, default=1,
        help="Parallel LLM calls in flight at once (default: 1).",
    )
    p.add_argument(
        "--min-party-amount", type=float, default=100.0,
        help="Materiality floor: drop parties whose total credited amount across "
             "matching vouchers is below this (default: 100; 0 disables). Kills "
             "paise-level rounding-entry parties.",
    )
    p.add_argument(
        "--party-config", type=Path, default=None,
        help="Party blocklist categories JSON "
             "(default: config/party_blocklist_categories.json).",
    )
    p.add_argument(
        "--party-report", type=Path, default=None,
        help="Party-filter audit report (default: party_blocklist_report.json "
             "next to --output).",
    )
    p.add_argument(
        "--party-cache", type=Path, default=None,
        help="Persistent party-decision cache (default: party_blocklist_cache.json "
             "next to --output).",
    )
    p.add_argument(
        "--no-party-filter", action="store_true",
        help="Skip the Stage-5 LLM party blocklist entirely.",
    )
    p.add_argument(
        "--party-dry-run", action="store_true",
        help="Run the party blocklist and write its audit report, but do NOT "
             "remove flagged names from the final list. Use on first rollout.",
    )
    p.add_argument(
        "--keep-on-llm-error", action="store_true",
        help="Do not abort when an LLM batch fails after retries (expense or "
             "party filter): keep its names conservatively and continue. This is "
             "the mode the webapp runs in.",
    )
    args = p.parse_args()

    try:
        run_tds_selection(
            ledgers=args.ledgers,
            daybook=args.daybook,
            groups_xml=args.groups_xml,
            config=args.config,
            output=args.output,
            report=args.report,
            filtered_expense=args.filtered_expense,
            cache=args.cache,
            model=args.model,
            batch_size=args.batch_size,
            max_tokens=args.max_tokens,
            no_thinking=args.no_thinking,
            no_reasons=args.no_reasons,
            concurrency=args.concurrency,
            as_json=args.json,
            no_group_exclusion=args.no_group_exclusion,
            fail_on_llm_error=not args.keep_on_llm_error,
            min_party_amount=args.min_party_amount,
            party_config=args.party_config,
            party_report=args.party_report,
            party_cache=args.party_cache,
            no_party_filter=args.no_party_filter,
            party_dry_run=args.party_dry_run,
        )
    except FileNotFoundError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
