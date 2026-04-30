#!/usr/bin/env python3
"""
Ledger names from vouchers_liability_no_expense_yes, excluding ledgers under selected
group roots (exclude_groups_ledgers). Defaults: tally_ledgers_final.xml, daybook XML, tally_groups_final.xml
beside this script; add --json for JSON output.

Expense/fixed-asset matching is inherited from vouchers_liability_no_expense_yes.py,
including exclusion of discount/round-off ledger names.

TDS mode (optional)
-------------------
Pass ``--filtered-expense FILE`` to load a pre-filtered expense set from a JSON array
or one-name-per-line text file (typically the output of ``apply_expense_blocklist.py``).
When given, that file is used as the expense_or_fixed set instead of the XML
classification. The liability_or_current set is still computed from XML. This
unlocks the manual-review TDS workflow:

  1. python apply_expense_blocklist.py --input expense_raw.json
     # writes expense_filtered.json + expense_blocklist_report.json
  2. # review expense_blocklist_report.json
  3. python final_list.py --filtered-expense expense_filtered.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Ensure sibling packages are importable regardless of working directory.
_ROOT = Path(__file__).resolve().parent.parent
for _d in ("classify", "vouchers", "tds"):
    _p = str(_ROOT / _d)
    if _p not in sys.path:
        sys.path.insert(0, _p)

from exclude_groups_ledgers import (
    DEFAULT_ROOT_GROUPS,
    ledgers_with_parent_in,
    parent_names_from_roots,
)
from vouchers_liability_no_expense_yes import (
    collect_matching_liability_names,
    load_expense_and_liability_sets,
)


def main() -> None:
    base = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--ledgers",
        type=Path,
        default=base / "tally_ledgers_final.xml",
        help="Enriched TALLYLEDGERS XML (default: tally_ledgers_final.xml next to script)",
    )
    p.add_argument(
        "--daybook",
        type=Path,
        default=base / "daybook_01042024_to_31032025.xml",
        help="TALLYDAYBOOK XML (default: daybook next to script)",
    )
    p.add_argument(
        "--groups-xml",
        type=Path,
        default=base / "tally_groups_final.xml",
        help="Groups XML for exclude-groups closure (default: tally_groups_final.xml)",
    )
    p.add_argument(
        "--filtered-expense",
        type=Path,
        default=None,
        help="Optional: pre-filtered expense ledger names "
             "(JSON array or one-name-per-line text). When given, this set replaces "
             "the XML-derived expense_or_fixed set (typically the output of "
             "apply_expense_blocklist.py for TDS analysis). The liability_or_current "
             "set still comes from XML. Beats auto-detect.",
    )
    p.add_argument(
        "--no-filter",
        action="store_true",
        help="Force raw-XML expense classification even if expense_filtered.json "
             "exists next to the ledgers XML. Use for non-TDS runs.",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Print a sorted JSON array instead of one name per line",
    )
    args = p.parse_args()

    if not args.ledgers.is_file():
        print(f"Ledgers file not found: {args.ledgers}", file=sys.stderr)
        sys.exit(1)
    if not args.daybook.is_file():
        print(f"Daybook file not found: {args.daybook}", file=sys.stderr)
        sys.exit(1)
    if not args.groups_xml.is_file():
        print(f"Groups file not found: {args.groups_xml}", file=sys.stderr)
        sys.exit(1)
    if args.filtered_expense is not None and not args.filtered_expense.is_file():
        print(f"Filtered expense file not found: {args.filtered_expense}", file=sys.stderr)
        sys.exit(1)

    ledgers_path = str(args.ledgers)
    # Both `--filtered-expense` and `--no-filter` are forwarded to the
    # centralized loader in vouchers_liability_no_expense_yes.py, which owns
    # the resolution logic (explicit override → no-filter opt-out → auto-detect
    # sidecar → raw XML).
    expense_or_fixed, liability_or_current = load_expense_and_liability_sets(
        args.ledgers,
        expense_override=args.filtered_expense,
        auto_detect=not args.no_filter,
    )
    voucher_names = collect_matching_liability_names(
        args.daybook, expense_or_fixed, liability_or_current
    )

    parent_names, _missing_roots = parent_names_from_roots(
        str(args.groups_xml), list(DEFAULT_ROOT_GROUPS)
    )
    duties_names = set(ledgers_with_parent_in(ledgers_path, parent_names))

    final_sorted = sorted(voucher_names - duties_names)

    if args.json:
        print(json.dumps(final_sorted, ensure_ascii=False, indent=2))
    else:
        for n in final_sorted:
            print(n)


if __name__ == "__main__":
    main()
