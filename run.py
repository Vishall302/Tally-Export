#!/usr/bin/env python3
"""
Single-entry pipeline runner for TALLY_EXPORT.

Standard offline pipeline (raw XMLs already in data/):
  python run.py

TDS mode — LLM expense blocklist before voucher scan (needs ANTHROPIC_API_KEY):
  python run.py --tds
  python run.py --tds --dry-run        # write audit report only, skip voucher scan

Export from live Tally first, then run offline pipeline:
  python run.py --export
  python run.py --export --start 01-04-2024 --end 31-03-2025

All flags can be combined:
  python run.py --export --tds --start 01-04-2024 --end 31-03-2025
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"


def step(label: str, cmd: list[str], stdout=None) -> None:
    print(f"\n{'─' * 60}", flush=True)
    print(f"  {label}", flush=True)
    print(f"{'─' * 60}", flush=True)
    result = subprocess.run(cmd, cwd=ROOT, stdout=stdout)
    if result.returncode != 0:
        print(f"\n[FAILED] {label} (exit {result.returncode})", file=sys.stderr, flush=True)
        sys.exit(result.returncode)


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--export", action="store_true",
        help="Run Tally export phase first (Tally must be open on localhost:9000)",
    )
    p.add_argument(
        "--start", default="01-04-2024", metavar="DD-MM-YYYY",
        help="Daybook start date for --export (default: %(default)s)",
    )
    p.add_argument(
        "--end", default="31-03-2025", metavar="DD-MM-YYYY",
        help="Daybook end date for --export (default: %(default)s)",
    )
    p.add_argument(
        "--tds", action="store_true",
        help="Apply LLM expense blocklist before voucher scan (TDS mode)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="TDS only: write audit report + filtered expense set, skip voucher scan",
    )
    args = p.parse_args()

    daybook_file = f"daybook_{args.start.replace('-', '')}_to_{args.end.replace('-', '')}.xml"
    LEDGERS       = DATA / "tally_ledgers_final.xml"
    DAYBOOK       = DATA / daybook_file
    GROUPS        = DATA / "tally_groups_final.xml"
    EXPENSE_FILT  = DATA / "expense_filtered.json"
    BLOCKLIST_RPT = DATA / "expense_blocklist_report.json"
    BLOCKLIST_CFG = ROOT / "config" / "expense_blocklist_categories.json"
    FINAL_LIST    = DATA / "final.txt"
    VOUCHERS_XML  = DATA / "vouchers_by_final_list"
    VOUCHERS_JSON = DATA / "vouchers_by_final_list_json"

    PY = sys.executable

    # ── Phase 1: Export from live Tally (optional) ────────────────────────────
    if args.export:
        step("Export  [1/3]  group hierarchy",
             [PY, "export/tally_groups.py"])
        step("Export  [2/3]  ledger master",
             [PY, "export/tally_ledger_master.py", "--out", str(LEDGERS)])
        step("Export  [3/3]  daybook",
             [PY, "export/tally_daybook.py",
              "--start", args.start, "--end", args.end, "--out", str(DAYBOOK)])

    # ── Phase 2: Ledger selection ─────────────────────────────────────────────
    if args.tds:
        tds_cmd = [
            PY, "tds/tds_expense_wrapper.py",
            "--ledgers",          str(LEDGERS),
            "--daybook",          str(DAYBOOK),
            "--groups-xml",       str(GROUPS),
            "--config",           str(BLOCKLIST_CFG),
            "--filtered-expense", str(EXPENSE_FILT),
            "--report",           str(BLOCKLIST_RPT),
            "--output",           str(FINAL_LIST),
        ]
        if args.dry_run:
            tds_cmd.append("--dry-run")
        step("TDS     [1/3]  blocklist filter + voucher scan", tds_cmd)
    else:
        with open(FINAL_LIST, "w") as out_f:
            step("Offline [1/3]  compute target ledger list",
                 [PY, "vouchers/final_list.py",
                  "--ledgers",   str(LEDGERS),
                  "--daybook",   str(DAYBOOK),
                  "--groups-xml", str(GROUPS)],
                 stdout=out_f)
        count = sum(1 for line in open(FINAL_LIST) if line.strip())
        print(f"  → {FINAL_LIST.relative_to(ROOT)}: {count} ledgers", flush=True)

    # ── Phase 3: Split + JSON (skipped on TDS dry-run) ───────────────────────
    if not (args.tds and args.dry_run):
        step("Offline [2/3]  split daybook into per-ledger XML slices",
             [PY, "vouchers/split_daybook_by_final_list.py",
              "--ledgers",   str(LEDGERS),
              "--daybook",   str(DAYBOOK),
              "--groups-xml", str(GROUPS),
              "--out-dir",   str(VOUCHERS_XML)])

        step("Offline [3/3]  convert XML slices to JSON",
             [PY, "vouchers/vouchers_to_json_with_ledger.py",
              "--ledgers",     str(LEDGERS),
              "--vouchers-dir", str(VOUCHERS_XML),
              "--output-dir",  str(VOUCHERS_JSON)])

    print(f"\n{'=' * 60}", flush=True)
    print("  Pipeline complete.", flush=True)
    print(f"{'=' * 60}\n", flush=True)


if __name__ == "__main__":
    main()
