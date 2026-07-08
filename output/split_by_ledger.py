#!/usr/bin/env python3
"""
Scan the daybook XML and write one ``TALLYDAYBOOK`` XML per final-list ledger,
containing every voucher that references that ledger (any ``LEDGERNAME`` under
the voucher, or ``PARTYLEDGERNAME`` on the voucher root).

Output files go under a folder (default: ``vouchers_by_final_list``) next to this script.
Filenames are sanitized copies of ledger names (unsafe characters replaced).

Where the ledger list comes from
--------------------------------
**Preferred (``--final-names FILE``):** load the exact final list from a file
(JSON array or one-name-per-line, e.g. the pipeline's ``final.txt``) and slice
those names verbatim — no re-derivation. This is the SINGLE SOURCE OF TRUTH and
the mode the ``run.py`` pipeline uses. It is the only way to honour the full TDS
selection, because the materiality floor (Stage 4.5) and the LLM party blocklist
(Stage 5) live ONLY inside ``tds/tds_expense_wrapper.py`` and are not reproduced
by ``load_final_ledger_names`` below.

**Fallback (no ``--final-names``):** re-derive the list via
``analyze/final_list.py::load_final_ledger_names``. WARNING: that path implements
only the 3 rule-based stages (expense blocklist → voucher scan → group
exclusion). It does NOT apply the materiality floor or the party blocklist, so a
TDS-mode run that omits ``--final-names`` would resurrect the dropped ledgers
(e.g. TDS Payable, ESIC Payable). Kept for standalone/offline back-compat only.

TDS mode (optional, fallback path only)
---------------------------------------
Pass ``--filtered-expense FILE`` to load a pre-filtered expense set from a JSON
array or one-name-per-line text file (typically the output of
``apply_expense_blocklist.py``). Only relevant when re-deriving (no
``--final-names``); ignored when ``--final-names`` is supplied.
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from analyze.final_list import load_final_ledger_names  # noqa: E402
from core.ledger_sets import load_name_list  # noqa: E402


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
    p.add_argument(
        "--final-names",
        type=Path,
        default=None,
        help="Authoritative final ledger list (JSON array or one-name-per-line, "
             "e.g. the pipeline's final.txt). When given, slice exactly these names "
             "and skip re-derivation — the ONLY mode that honours the materiality "
             "floor + party blocklist. Pipelines should always pass this.",
    )
    p.add_argument(
        "--filtered-expense",
        type=Path,
        default=None,
        help="Optional: pre-filtered expense ledger names "
             "(JSON array or one-name-per-line text). When given, this set replaces "
             "the XML-derived expense_or_fixed set (typically the output of "
             "apply_expense_blocklist.py for TDS analysis). Beats auto-detect.",
    )
    p.add_argument(
        "--no-filter",
        action="store_true",
        help="Force raw-XML expense classification even if expense_filtered.json "
             "exists next to the ledgers XML. Use for non-TDS runs.",
    )
    args = p.parse_args()

    if not args.daybook.is_file():
        print(f"Daybook file not found: {args.daybook}", file=sys.stderr)
        sys.exit(1)

    if args.final_names is not None:
        # Preferred path: consume the authoritative list verbatim (no re-derivation).
        if not args.final_names.is_file():
            print(f"Final-names file not found: {args.final_names}", file=sys.stderr)
            sys.exit(1)
        # Sort for deterministic filenames + duplicate-stem numbering (final.txt is
        # already sorted; sorting a set from JSON makes order independent of format).
        names = sorted(load_name_list(args.final_names))
    else:
        # Fallback path: re-derive with the 3 rule-based stages only (no floor /
        # party blocklist). Needs the ledger + groups XML.
        if not args.ledgers.is_file():
            print(f"Ledgers file not found: {args.ledgers}", file=sys.stderr)
            sys.exit(1)
        if not args.groups_xml.is_file():
            print(f"Groups file not found: {args.groups_xml}", file=sys.stderr)
            sys.exit(1)
        if args.filtered_expense is not None and not args.filtered_expense.is_file():
            print(f"Filtered expense file not found: {args.filtered_expense}", file=sys.stderr)
            sys.exit(1)
        names = load_final_ledger_names(
            args.ledgers,
            args.daybook,
            args.groups_xml,
            args.filtered_expense,
            auto_detect=not args.no_filter,
        )
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
