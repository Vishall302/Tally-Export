#!/usr/bin/env python3
"""
List Tally ledger names that are either liability-type or under Current Assets.

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

Why iterparse
    Uses ``xml.etree.ElementTree.iterparse`` and clears each ``LEDGER`` element
    after processing so multi‑GB exports stay within reasonable memory.

CLI
    ``python list_liability_or_current_assets_ledgers.py [path/to/ledger.xml]``
    Default XML path is ``tally_ledgers_final.xml`` in the current directory.

Exit codes
    ``0`` on success; ``1`` if the file is missing or XML parsing fails (errors
    on stderr).
"""

from __future__ import annotations

import argparse
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
    args = parser.parse_args()

    matches: list[str] = []
    try:
        for _event, elem in ET.iterparse(args.xml_path, events=("end",)):
            if elem.tag != "LEDGER":
                continue
            name = (elem.get("NAME") or "").strip()
            nature = (elem.findtext("NATURE") or "").strip()
            rootprimary = (elem.findtext("ROOTPRIMARY") or "").strip()
            if nature == "Liability" or rootprimary == "Current Assets":
                matches.append(name)
            elem.clear()
    except FileNotFoundError:
        print(f"File not found: {args.xml_path}", file=sys.stderr)
        sys.exit(1)
    except ET.ParseError as e:
        print(f"XML parse error: {e}", file=sys.stderr)
        sys.exit(1)

    matches.sort()
    for n in matches:
        print(n)


if __name__ == "__main__":
    main()
