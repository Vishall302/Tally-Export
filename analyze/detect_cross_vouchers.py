#!/usr/bin/env python3
"""
Scan a Tally daybook export and list **liability / current-asset** ledger names that
appear in "mixed" journal-style vouchers.

Background (Tally XML)
----------------------
- **Daybook** (`TALLYDAYBOOK`): contains `<VOUCHER>` elements. Each voucher has
  `<LEDGERENTRIES>` with one `<ENTRY>` per ledger line.
- **Ledgers** (`TALLYLEDGERS`): enriched export lists each `<LEDGER>` with
  `NATURE` and `ROOTPRIMARY` so we can classify ledgers the same way as the
  standalone list scripts.

**ISDEEMEDPOSITIVE** on an entry (not on the voucher root):
  - `Yes` → debit-like line in Tally's accounting sense for that entry.
  - `No`  → credit-like line.

What this script finds
----------------------
For each **voucher**, we test **all ledger lines in that voucher together**:

  1) There is at least one line where:
     - `LEDGERNAME` is classified as **Expense OR under Fixed Assets** (same rule as
       ``list_expense_ledgers.py``), and
     - `ISDEEMEDPOSITIVE` is **Yes**.

  2) There is at least one line where:
     - `LEDGERNAME` is classified as **Liability OR under Current Assets** (same rule as
       ``list_liability_ledgers.py``), and
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

TDS mode (auto-detect + opt-out)
--------------------------------
By default, every consumer of ``load_expense_and_liability_sets()`` auto-detects
a sidecar file named ``expense_filtered.json`` next to the ledger master XML
(typically the output of ``apply_expense_blocklist.py``). Resolution order:

  1. Explicit ``--filtered-expense FILE`` argument wins.
  2. ``--no-filter`` flag forces raw XML even if a sidecar exists.
  3. Otherwise, if ``<ledgers_xml.parent>/expense_filtered.json`` exists,
     it is loaded automatically.
  4. Otherwise, the expense set is built from XML (original behavior).

Output
------
  - Default: write one ledger name per line (UTF‑8), sorted, to ``test.txt``
    next to this script (override with ``-o`` / ``--output``).
  - ``--json``: UTF‑8 JSON array of strings, sorted, same output file rules.

Usage
-----
  python detect_cross_vouchers.py
  python detect_cross_vouchers.py --daybook path.xml --ledgers path.xml
  python detect_cross_vouchers.py --json
  python detect_cross_vouchers.py -o /path/to/out.txt
  python detect_cross_vouchers.py --filtered-expense expense_filtered.json
"""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.ledger_sets import (  # noqa: E402
    DEFAULT_FILTERED_EXPENSE_NAME,
    load_expense_and_liability_sets,
)


def collect_matching_liability_amounts(
    daybook_xml: Path,
    expense_or_fixed: set[str],
    liability_or_current: set[str],
) -> dict[str, float]:
    """
    Walk every ``<VOUCHER>`` in the daybook; for vouchers that match the
    expense‑Yes + liability‑No pattern, collect all liability/current‑asset
    ledger names that appeared as ``ISDEEMEDPOSITIVE`` No in that voucher,
    accumulating ``sum(abs(AMOUNT))`` per name across all matching vouchers.

    Absolute amounts are summed on purpose: a credit and its reversal should
    make a name *more* visible for review, not net out to zero and vanish.
    A name credited with a missing/unparseable AMOUNT still appears (with 0.0),
    so ``set(result)`` is exactly the name set the original scan produced.
    """
    out: dict[str, float] = {}
    for _event, voucher in ET.iterparse(str(daybook_xml), events=("end",)):
        if voucher.tag != "VOUCHER":
            continue

        expense_yes = False
        liability_no_lines: list[tuple[str, float]] = []

        ledge = voucher.find("LEDGERENTRIES")
        if ledge is not None:
            for entry in ledge.findall("ENTRY"):
                lname = (entry.findtext("LEDGERNAME") or "").strip()
                deemed = (entry.findtext("ISDEEMEDPOSITIVE") or "").strip()
                if lname in expense_or_fixed and deemed == "Yes":
                    expense_yes = True
                if lname in liability_or_current and deemed == "No":
                    try:
                        amount = abs(float((entry.findtext("AMOUNT") or "0").strip() or 0))
                    except ValueError:
                        amount = 0.0
                    liability_no_lines.append((lname, amount))

        if expense_yes and liability_no_lines:
            for lname, amount in liability_no_lines:
                out[lname] = out.get(lname, 0.0) + amount

        voucher.clear()

    return out


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
    return set(
        collect_matching_liability_amounts(
            daybook_xml, expense_or_fixed, liability_or_current
        )
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
    p.add_argument(
        "--filtered-expense",
        type=Path,
        default=None,
        help="Optional: pre-filtered expense ledger names "
             "(JSON array or one-name-per-line text). When given, this set replaces "
             "the XML-derived expense_or_fixed set (typically the output of "
             "apply_expense_blocklist.py for TDS analysis). The liability/current-assets "
             "set is still computed from XML. Beats auto-detect.",
    )
    p.add_argument(
        "--no-filter",
        action="store_true",
        help="Force raw-XML expense classification even if a sidecar "
             f"({DEFAULT_FILTERED_EXPENSE_NAME}) exists next to the ledgers XML. "
             "Use this for non-TDS runs when you don't want the auto-detected "
             "filter to silently take effect. Ignored when --filtered-expense is also passed.",
    )
    args = p.parse_args()

    if not args.ledgers.is_file():
        print(f"Ledgers file not found: {args.ledgers}", file=sys.stderr)
        sys.exit(1)
    if not args.daybook.is_file():
        print(f"Daybook file not found: {args.daybook}", file=sys.stderr)
        sys.exit(1)
    if args.filtered_expense is not None and not args.filtered_expense.is_file():
        print(f"Filtered expense file not found: {args.filtered_expense}", file=sys.stderr)
        sys.exit(1)

    expense_or_fixed, liability_or_current = load_expense_and_liability_sets(
        args.ledgers,
        expense_override=args.filtered_expense,
        auto_detect=not args.no_filter,
    )
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
