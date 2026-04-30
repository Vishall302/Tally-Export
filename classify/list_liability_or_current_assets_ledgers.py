#!/usr/bin/env python3
"""
List Tally ledger names that are either liability-type or under Current Assets,
and that also appear at least once in daybook voucher ledger entries.

What it reads
    Tally ledger export XML (typically ``tally_ledgers_final.xml``) containing
    ``<LEDGER>`` elements with attributes ``NAME`` and child tags ``NATURE``
    and ``ROOTPRIMARY`` (Tally group hierarchy).

Selection rule (OR)
    A ledger is included if either:
    - ``NATURE`` text equals exactly ``Liability``, or
    - ``ROOTPRIMARY`` text equals exactly ``Current Assets``.

Output
    One ledger name per line to stdout, sorted alphabetically. No header.
    Includes only names present in voucher entries from daybook XML.

Why iterparse
    Uses ``xml.etree.ElementTree.iterparse`` and clears each ``LEDGER`` element
    after processing so multi‑GB exports stay within reasonable memory.

CLI
    ``python list_liability_or_current_assets_ledgers.py [path/to/ledger.xml] [path/to/daybook.xml]``
    Default ledger XML path is ``tally_ledgers_final.xml`` in the current directory.
    Default daybook XML path is ``daybook_01042024_to_31032025.xml`` in the current directory.

Exit codes
    ``0`` on success; ``1`` if the file is missing or XML parsing fails (errors
    on stderr).
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import xml.etree.ElementTree as ET


def main() -> None:
    parser = argparse.ArgumentParser(
        description='List ledger names: NATURE=Liability OR ROOTPRIMARY=Current Assets'
    )
    parser.add_argument(
        "xml_path",
        nargs="?",
        default="tally_ledgers_final.xml",
        help="Path to TALLYLEDGERS XML (default: tally_ledgers_final.xml)",
    )
    parser.add_argument(
        "daybook_path",
        nargs="?",
        default="daybook_01042024_to_31032025.xml",
        help="Path to daybook XML (default: daybook_01042024_to_31032025.xml)",
    )
    args = parser.parse_args()
    xml_path = Path(args.xml_path)
    daybook_path = Path(args.daybook_path)

    if not xml_path.is_file():
        print(f"File not found: {xml_path}", file=sys.stderr)
        sys.exit(1)
    if not daybook_path.is_file():
        print(f"File not found: {daybook_path}", file=sys.stderr)
        sys.exit(1)

    matches: set[str] = set()
    voucher_ledger_names: set[str] = set()
    try:
        daybook_tree = ET.parse(daybook_path)
        daybook_root = daybook_tree.getroot()
        for el in daybook_root.findall(".//LEDGERENTRIES/ENTRY/LEDGERNAME"):
            name = (el.text or "").strip()
            if name:
                voucher_ledger_names.add(name)

        for _event, elem in ET.iterparse(xml_path, events=("end",)):
            if elem.tag != "LEDGER":
                continue
            name = (elem.get("NAME") or "").strip()
            nature = (elem.findtext("NATURE") or "").strip()
            rootprimary = (elem.findtext("ROOTPRIMARY") or "").strip()
            if (nature == "Liability" or rootprimary == "Current Assets") and name in voucher_ledger_names:
                matches.add(name)
            elem.clear()
    except ET.ParseError as e:
        print(f"XML parse error: {e}", file=sys.stderr)
        sys.exit(1)

    sorted_matches = sorted(matches)
    for n in sorted_matches:
        print(n)


if __name__ == "__main__":
    main()
