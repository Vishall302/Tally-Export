#!/usr/bin/env python3
"""
Scan a Tally daybook export and list **liability / current-asset** ledger names that
appear in “mixed” journal-style vouchers.

Background (Tally XML)
----------------------
- **Daybook** (`TALLYDAYBOOK`): contains `<VOUCHER>` elements. Each voucher has
  `<LEDGERENTRIES>` with one `<ENTRY>` per ledger line.
- **Ledgers** (`TALLYLEDGERS`): enriched export lists each `<LEDGER>` with
  `NATURE` and `ROOTPRIMARY` so we can classify ledgers the same way as the
  standalone list scripts.

**ISDEEMEDPOSITIVE** on an entry (not on the voucher root):
  - `Yes` → debit-like line in Tally’s accounting sense for that entry.
  - `No`  → credit-like line.

What this script finds
----------------------
For each **voucher**, we test **all ledger lines in that voucher together**:

  1) There is at least one line where:
     - `LEDGERNAME` is classified as **Expense OR under Fixed Assets** (same rule as
       ``list_expense_or_fixed_asset_ledgers.py``), and
     - `ISDEEMEDPOSITIVE` is **Yes**.

  2) There is at least one line where:
     - `LEDGERNAME` is classified as **Liability OR under Current Assets** (same rule as
       ``list_liability_or_current_assets_ledgers.py``), and
     - `ISDEEMEDPOSITIVE` is **No**.

If **both** (1) and (2) hold in the **same** voucher, we record every distinct
liability/current-asset ledger name from the lines that satisfy (2) for that voucher.
Across the whole daybook, names are **deduplicated** (union), then **sorted** for output.

Ledger classification (must match the two list scripts)
--------------------------------------------------------
  - Expense / Fixed Assets set: ``NATURE == "Expense"`` OR ``ROOTPRIMARY == "Fixed Assets"``.
  - Liability / Current Assets set: ``NATURE == "Liability"`` OR ``ROOTPRIMARY == "Current Assets"``.

Performance
------------
Both XML files are read with **iterparse** and elements are **cleared** after use so
large exports (multi‑million lines) stay within reasonable memory.

Inputs / defaults
------------------
  - Ledgers: ``tally_ledgers_final.xml`` (same folder as this script unless ``--ledgers``).
  - Daybook: ``daybook_01042024_to_31032025.xml`` unless ``--daybook``.

Output
------
  - Default: write one ledger name per line (UTF‑8), sorted, to ``test.txt``
    next to this script (override with ``-o`` / ``--output``).
  - ``--json``: UTF‑8 JSON array of strings, sorted, same output file rules.

Usage
-----
  python vouchers_liability_no_expense_yes.py
  python vouchers_liability_no_expense_yes.py --daybook path.xml --ledgers path.xml
  python vouchers_liability_no_expense_yes.py --json
  python vouchers_liability_no_expense_yes.py -o /path/to/out.txt
"""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def load_expense_and_liability_sets(ledgers_xml: Path) -> tuple[set[str], set[str]]:
    """
    Build two sets of ledger display names from the enriched ledgers XML.

    Mirrors:
      - ``list_expense_or_fixed_asset_ledgers.py``  → first set
      - ``list_liability_or_current_assets_ledgers.py`` → second set

    Uses incremental parsing and clears each ``<LEDGER>`` after processing to limit
    memory use on large files.
    """
    expense_or_fixed: set[str] = set()
    liability_or_current: set[str] = set()
    for _event, elem in ET.iterparse(str(ledgers_xml), events=("end",)):
        if elem.tag != "LEDGER":
            continue
        name = (elem.get("NAME") or "").strip()
        if not name:
            elem.clear()
            continue
        nature = (elem.findtext("NATURE") or "").strip()
        rootprimary = (elem.findtext("ROOTPRIMARY") or "").strip()
        if nature == "Expense" or rootprimary == "Fixed Assets":
            expense_or_fixed.add(name)
        if nature == "Liability" or rootprimary == "Current Assets":
            liability_or_current.add(name)
        elem.clear()
    return expense_or_fixed, liability_or_current


def collect_matching_liability_names(
    daybook_xml: Path,
    expense_or_fixed: set[str],
    liability_or_current: set[str],
) -> set[str]:
    """
    Walk every ``<VOUCHER>`` in the daybook; for vouchers that match the
    expense‑Yes + liability‑No pattern, collect all liability/current‑asset
    ledger names that appeared as ``ISDEEMEDPOSITIVE`` No in that voucher.

    Returns a set (unique names across all matching vouchers).
    """
    out: set[str] = set()
    for _event, voucher in ET.iterparse(str(daybook_xml), events=("end",)):
        if voucher.tag != "VOUCHER":
            continue

        expense_yes = False
        liability_no_names: list[str] = []

        ledge = voucher.find("LEDGERENTRIES")
        if ledge is not None:
            for entry in ledge.findall("ENTRY"):
                lname = (entry.findtext("LEDGERNAME") or "").strip()
                deemed = (entry.findtext("ISDEEMEDPOSITIVE") or "").strip()
                if lname in expense_or_fixed and deemed == "Yes":
                    expense_yes = True
                if lname in liability_or_current and deemed == "No":
                    liability_no_names.append(lname)

        if expense_yes and liability_no_names:
            out.update(liability_no_names)

        voucher.clear()

    return out


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
        "--json",
        action="store_true",
        help="Write a sorted JSON array instead of one name per line",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=base / "test.txt",
        help="Write the result list to this file (default: test.txt next to this script)",
    )
    args = p.parse_args()

    if not args.ledgers.is_file():
        print(f"Ledgers file not found: {args.ledgers}", file=sys.stderr)
        sys.exit(1)
    if not args.daybook.is_file():
        print(f"Daybook file not found: {args.daybook}", file=sys.stderr)
        sys.exit(1)

    expense_or_fixed, liability_or_current = load_expense_and_liability_sets(args.ledgers)
    names = collect_matching_liability_names(
        args.daybook, expense_or_fixed, liability_or_current
    )
    sorted_names = sorted(names)

    with args.output.open("w", encoding="utf-8", newline="\n") as f:
        if args.json:
            f.write(json.dumps(sorted_names, ensure_ascii=False, indent=2))
            f.write("\n")
        else:
            for n in sorted_names:
                f.write(n + "\n")


if __name__ == "__main__":
    main()
