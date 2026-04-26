#!/usr/bin/env python3
"""
For each ledger name produced by ``final_list.py`` (same logic and defaults), scan
``daybook_01042024_to_31032025.xml`` and write one ``TALLYDAYBOOK`` XML per name
containing every voucher that references that ledger (any ``LEDGERNAME`` under the
voucher, or ``PARTYLEDGERNAME`` on the voucher root).

Output files go under a folder (default: ``vouchers_by_final_list``) next to this script.
Filenames are sanitized copies of ledger names (unsafe characters replaced).

Inherited filtering from final_list.py includes exclusion of discount/round-off
ledgers in the expense/fixed-asset side of the voucher pattern.
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

from exclude_groups_ledgers import (
    DEFAULT_ROOT_GROUPS,
    ledgers_with_parent_in,
    parent_names_from_roots,
)
from vouchers_liability_no_expense_yes import (
    collect_matching_liability_names,
    load_expense_and_liability_sets,
)


def load_final_ledger_names(
    ledgers: Path,
    daybook: Path,
    groups_xml: Path,
) -> list[str]:
    ledgers_path = str(ledgers)
    expense_or_fixed, liability_or_current = load_expense_and_liability_sets(ledgers)
    voucher_names = collect_matching_liability_names(
        daybook, expense_or_fixed, liability_or_current
    )
    parent_names, _missing_roots = parent_names_from_roots(
        str(groups_xml), list(DEFAULT_ROOT_GROUPS)
    )
    duties_names = set(ledgers_with_parent_in(ledgers_path, parent_names))
    return sorted(voucher_names - duties_names)


def read_tally_daybook_root_attribs(daybook_xml: Path) -> dict[str, str]:
    with daybook_xml.open("rb") as f:
        it = ET.iterparse(f, events=("start",))
        for _event, elem in it:
            if elem.tag == "TALLYDAYBOOK":
                return dict(elem.attrib)
    raise ValueError(f"No TALLYDAYBOOK root in {daybook_xml}")


def ledger_names_in_voucher(voucher: ET.Element) -> set[str]:
    names: set[str] = set()
    party = (voucher.findtext("PARTYLEDGERNAME") or "").strip()
    if party:
        names.add(party)
    for el in voucher.findall(".//LEDGERNAME"):
        t = (el.text or "").strip()
        if t:
            names.add(t)
    return names


def sanitize_filename(name: str) -> str:
    bad = '<>:"/\\|?*\n\r\t'
    s = "".join("_" if c in bad else c for c in name).strip()
    return s or "ledger"


def write_daybook_subset(
    out_path: Path,
    root_attrib: dict[str, str],
    voucher_xml_strings: list[str],
) -> None:
    total = len(voucher_xml_strings)
    merged = {**root_attrib, "TOTALCOUNT": str(total)}
    root = ET.Element("TALLYDAYBOOK", merged)
    for vs in voucher_xml_strings:
        root.append(ET.fromstring(vs))
    ET.indent(root, space="  ")
    tree = ET.ElementTree(root)
    tree.write(out_path, encoding="utf-8", xml_declaration=True)


def build_index_and_vouchers(
    daybook_xml: Path,
    targets: set[str],
) -> tuple[list[str], dict[str, list[int]]]:
    voucher_strings: list[str] = []
    by_name: dict[str, list[int]] = defaultdict(list)

    for _event, voucher in ET.iterparse(str(daybook_xml), events=("end",)):
        if voucher.tag != "VOUCHER":
            continue
        names = ledger_names_in_voucher(voucher)
        idx = len(voucher_strings)
        vxml = ET.tostring(voucher, encoding="unicode")
        voucher_strings.append(vxml)
        for n in names:
            if n in targets:
                by_name[n].append(idx)

        voucher.clear()

    return voucher_strings, dict(by_name)


def main() -> None:
    base = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--ledgers",
        type=Path,
        default=base / "tally_ledgers_final.xml",
        help="Enriched TALLYLEDGERS XML",
    )
    p.add_argument(
        "--daybook",
        type=Path,
        default=base / "daybook_01042024_to_31032025.xml",
        help="TALLYDAYBOOK XML",
    )
    p.add_argument(
        "--groups-xml",
        type=Path,
        default=base / "tally_groups_final.xml",
        help="Groups XML for exclude-groups closure",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=base / "vouchers_by_final_list",
        help="Folder for one XML per final-list ledger (created if missing)",
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

    names = load_final_ledger_names(args.ledgers, args.daybook, args.groups_xml)
    if not names:
        print("Final list is empty; nothing to write.", file=sys.stderr)
        sys.exit(0)

    targets = set(names)
    root_attrib = read_tally_daybook_root_attribs(args.daybook)
    voucher_strings, by_name = build_index_and_vouchers(args.daybook, targets)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    stem_seq: defaultdict[str, int] = defaultdict(int)

    written = 0
    for display_name in names:
        idxs = by_name.get(display_name, [])
        if not idxs:
            subset: list[str] = []
        else:
            subset = [voucher_strings[i] for i in idxs]
        stem = sanitize_filename(display_name)
        stem_seq[stem] += 1
        n = stem_seq[stem]
        out_name = f"{stem}.xml" if n == 1 else f"{stem}_{n}.xml"
        out_path = args.out_dir / out_name
        write_daybook_subset(out_path, root_attrib, subset)
        written += 1

    print(
        f"Wrote {written} file(s) under {args.out_dir} using {len(voucher_strings)} vouchers from daybook."
    )


if __name__ == "__main__":
    main()
