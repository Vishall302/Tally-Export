#!/usr/bin/env python3
"""
Extract ledger names from Tally enriched export XML (e.g. from tally_ledger_master.py).

Each <LEDGER> has display name on the NAME attribute; NATURE and ROOTPRIMARY are
siblings under the ledger root (inserted when export uses enrichment).

Filter (OR — not AND): include a ledger if either is true:
  - <NATURE> text equals "Expense", or
  - <ROOTPRIMARY> text equals "Fixed Assets"
So the output is the union of (all expense-nature ledgers) and (all under Fixed Assets),
e.g. an expense ledger does not also need to be a fixed asset.

Output: document order, one name per line (or --json for a UTF-8 JSON array).
By default, only names that also appear in voucher ledger entries are included.
Redirect to a file if needed, e.g.  ... > test3.txt

Usage:
  python list_expense_or_fixed_asset_ledgers.py
  python list_expense_or_fixed_asset_ledgers.py --xml /path/to/tally_ledgers_final.xml
  python list_expense_or_fixed_asset_ledgers.py --daybook /path/to/daybook.xml
  python list_expense_or_fixed_asset_ledgers.py --json
"""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--xml",
        type=Path,
        default=Path(__file__).resolve().parent / "tally_ledgers_final.xml",
        help="Path to enriched Tally ledgers XML (default: alongside this script)",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit a JSON array instead of one name per line",
    )
    p.add_argument(
        "--daybook",
        type=Path,
        default=Path(__file__).resolve().parent / "daybook_01042024_to_31032025.xml",
        help="Path to daybook XML for voucher ledger filtering (default: alongside this script)",
    )
    args = p.parse_args()

    if not args.xml.is_file():
        print(f"File not found: {args.xml}", file=sys.stderr)
        sys.exit(1)
    if not args.daybook.is_file():
        print(f"File not found: {args.daybook}", file=sys.stderr)
        sys.exit(1)

    tree = ET.parse(args.xml)
    tally = tree.getroot()  # <TALLYLEDGERS> wrapper
    daybook_tree = ET.parse(args.daybook)
    daybook = daybook_tree.getroot()

    voucher_ledger_names: set[str] = set()
    for el in daybook.findall(".//LEDGERENTRIES/ENTRY/LEDGERNAME"):
        name = (el.text or "").strip()
        if name:
            voucher_ledger_names.add(name)

    names: list[str] = []
    seen: set[str] = set()
    for ledger in tally.findall("LEDGER"):
        # Direct children only — avoids nested <NATURE> inside e.g. GSTDETAILS
        nature_el = ledger.find("NATURE")
        root_el = ledger.find("ROOTPRIMARY")
        nature = (nature_el.text or "").strip() if nature_el is not None else ""
        rootprimary = (root_el.text or "").strip() if root_el is not None else ""

        if nature == "Expense" or rootprimary == "Fixed Assets":
            name = (ledger.get("NAME") or "").strip()  # ledger display name from XML attribute
            if name and name in voucher_ledger_names and name not in seen:
                names.append(name)
                seen.add(name)

    if args.json:
        print(json.dumps(names, ensure_ascii=False, indent=2))
    else:
        for n in names:
            print(n)

if __name__ == "__main__":
    main()
