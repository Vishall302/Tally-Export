#!/usr/bin/env python3
"""
Convert each ``vouchers_by_final_list/*.xml`` daybook subset to JSON, prepending
master details for that ledger from ``tally_ledgers_final.xml``. Tally often stores
GST, PAN, state, pincode, etc. under **several** tags (for example ``PARTYGSTIN``,
``LEDGSTREGDETAILS.LIST`` / ``GSTIN``, repeated registration blocks); this script
resolves one canonical value per concept and records optional ``*_all_distinct`` /
``field_sources`` when multiple differing values exist.

Input filenames are derived from ledger names (see ``split_daybook_by_final_list.py``);
the file stem must match the ``NAME`` attribute on ``<LEDGER>`` in the master file.

Output default: ``vouchers_by_final_list_json/`` next to this script (mirrors input names
with ``.json`` extension). Uses only the standard library.
"""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import Any


# Scalar tags copied as-is (no multi-location merge). Tax IDs below are filled via resolvers.
_LEDGER_DETAIL_TAGS: frozenset[str] = frozenset(
    {
        "GUID",
        "PARENT",
        "ROOTPRIMARY",
        "NATURE",
        "FINANCIALSTATEMENT",
        "GSTAPPLICABLE",
        "TAXCLASSIFICATIONNAME",
        "TAXTYPE",
        "GSTTYPE",
        "GSTTYPEOFSUPPLY",
        "GSTNATUREOFSUPPLY",
        "VATDEALERTYPE",
    }
)


def _find_direct_child(parent: ET.Element, tag: str) -> ET.Element | None:
    for c in parent:
        if c.tag == tag:
            return c
    return None


def _direct_text(ledger: ET.Element, tag: str) -> str:
    el = _find_direct_child(ledger, tag)
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _children_tagged(parent: ET.Element, tag: str) -> list[ET.Element]:
    return [c for c in parent if c.tag == tag]


def _ordered_distinct(values: list[str]) -> list[str]:
    return list(dict.fromkeys([v for v in values if v]))


def _collect_gstin_source_pairs(ledger: ET.Element) -> list[tuple[str, str]]:
    """
    Tally repeats GST identifiers under PARTYGSTIN and under each LEDGSTREGDETAILS.LIST.
    Order defines preference for the single canonical GSTIN.
    """
    pairs: list[tuple[str, str]] = []
    party = _direct_text(ledger, "PARTYGSTIN")
    if party:
        pairs.append(("PARTYGSTIN", party))
    for i, block in enumerate(_children_tagged(ledger, "LEDGSTREGDETAILS.LIST")):
        v = _direct_text(block, "GSTIN")
        if v:
            pairs.append((f"LEDGSTREGDETAILS.LIST[{i}].GSTIN", v))
    return pairs


def _collect_pan_values(ledger: ET.Element) -> list[str]:
    """INCOMETAXNUMBER may appear once on the ledger or in repeated blocks — collect all."""
    vals: list[str] = []
    for el in ledger.iter("INCOMETAXNUMBER"):
        if el.text and el.text.strip():
            vals.append(el.text.strip())
    return vals


def _resolve_gst_registration_type(ledger: ET.Element) -> tuple[str, str]:
    seq: list[tuple[str, str]] = []
    v = _direct_text(ledger, "GSTREGISTRATIONTYPE")
    if v:
        seq.append(("GSTREGISTRATIONTYPE", v))
    v = _direct_text(ledger, "VATDEALERTYPE")
    if v:
        seq.append(("VATDEALERTYPE", v))
    for i, block in enumerate(_children_tagged(ledger, "LEDGSTREGDETAILS.LIST")):
        t = _direct_text(block, "GSTREGISTRATIONTYPE")
        if t:
            seq.append((f"LEDGSTREGDETAILS.LIST[{i}].GSTREGISTRATIONTYPE", t))
    for src, val in seq:
        if val:
            return val, src
    return "", ""


def _resolve_state(ledger: ET.Element) -> tuple[str, str]:
    v = _direct_text(ledger, "PRIORSTATENAME")
    if v:
        return v, "PRIORSTATENAME"
    for i, block in enumerate(_children_tagged(ledger, "LEDMAILINGDETAILS.LIST")):
        t = _direct_text(block, "STATE")
        if t:
            return t, f"LEDMAILINGDETAILS.LIST[{i}].STATE"
    for i, block in enumerate(_children_tagged(ledger, "LEDGSTREGDETAILS.LIST")):
        t = _direct_text(block, "STATE")
        if t:
            return t, f"LEDGSTREGDETAILS.LIST[{i}].STATE"
    return "", ""


def _resolve_place_of_supply(ledger: ET.Element) -> tuple[str, str]:
    for i, block in enumerate(_children_tagged(ledger, "LEDGSTREGDETAILS.LIST")):
        t = _direct_text(block, "PLACEOFSUPPLY")
        if t:
            return t, f"LEDGSTREGDETAILS.LIST[{i}].PLACEOFSUPPLY"
    return "", ""


def _resolve_pincode(ledger: ET.Element) -> tuple[str, str]:
    v = _direct_text(ledger, "PINCODE")
    if v:
        return v, "PINCODE"
    for i, block in enumerate(_children_tagged(ledger, "LEDMAILINGDETAILS.LIST")):
        t = _direct_text(block, "PINCODE")
        if t:
            return t, f"LEDMAILINGDETAILS.LIST[{i}].PINCODE"
    return "", ""


def _resolve_mailing_name(ledger: ET.Element) -> tuple[str, str]:
    v = _direct_text(ledger, "MAILINGNAME")
    if v:
        return v, "MAILINGNAME"
    for i, block in enumerate(_children_tagged(ledger, "LEDMAILINGDETAILS.LIST")):
        t = _direct_text(block, "MAILINGNAME")
        if t:
            return t, f"LEDMAILINGDETAILS.LIST[{i}].MAILINGNAME"
    ln = _find_direct_child(ledger, "LANGUAGENAME.LIST")
    if ln is not None:
        nl = _find_direct_child(ln, "NAME.LIST")
        if nl is not None:
            nm = _find_direct_child(nl, "NAME")
            if nm is not None and nm.text and nm.text.strip():
                return nm.text.strip(), "LANGUAGENAME.LIST/NAME.LIST/NAME"
    return "", ""


def _resolve_country(ledger: ET.Element) -> tuple[str, str]:
    v = _direct_text(ledger, "COUNTRYNAME")
    if v:
        return v, "COUNTRYNAME"
    for i, block in enumerate(_children_tagged(ledger, "LEDMAILINGDETAILS.LIST")):
        t = _direct_text(block, "COUNTRY")
        if t:
            return t, f"LEDMAILINGDETAILS.LIST[{i}].COUNTRY"
    return "", ""


def extract_ledger_master_fields(ledger: ET.Element) -> dict[str, Any]:
    """
    Flatten master fields from a <LEDGER>, resolving duplicates where Tally stores the
    same fact under several tags (e.g. GSTIN under PARTYGSTIN vs LEDGSTREGDETAILS.LIST).
    """
    name = (ledger.attrib.get("NAME") or "").strip()
    out: dict[str, Any] = {"NAME": name}
    sources: dict[str, str] = {}

    for tag in _LEDGER_DETAIL_TAGS:
        el = _find_direct_child(ledger, tag)
        if el is not None and el.text and el.text.strip():
            out[tag] = el.text.strip()

    gst_pairs = _collect_gstin_source_pairs(ledger)
    gst_distinct = _ordered_distinct([v for _src, v in gst_pairs])
    if gst_distinct:
        primary = gst_distinct[0]
        out["GSTIN"] = primary
        out["PARTYGSTIN"] = _direct_text(ledger, "PARTYGSTIN") or primary
        if len(gst_distinct) > 1:
            out["GSTIN_all_distinct"] = gst_distinct
        for src, val in gst_pairs:
            if val == primary:
                sources["GSTIN"] = src
                break

    pans = _ordered_distinct(_collect_pan_values(ledger))
    direct_pan = _direct_text(ledger, "INCOMETAXNUMBER")
    if pans:
        out["PAN"] = pans[0]
        out["INCOMETAXNUMBER"] = pans[0]
        if len(pans) > 1:
            out["PAN_all_distinct"] = pans
        if direct_pan and direct_pan == pans[0]:
            sources["PAN"] = "INCOMETAXNUMBER"
        elif direct_pan:
            sources["PAN"] = "INCOMETAXNUMBER(first_of_multiple_locations)"
        else:
            sources["PAN"] = "INCOMETAXNUMBER(nested)"

    grt, grt_src = _resolve_gst_registration_type(ledger)
    if grt:
        out["GSTREGISTRATIONTYPE"] = grt
        sources["GSTREGISTRATIONTYPE"] = grt_src

    st, st_src = _resolve_state(ledger)
    if st:
        out["STATE"] = st
        out["PRIORSTATENAME"] = _direct_text(ledger, "PRIORSTATENAME") or st
        sources["STATE"] = st_src

    pos, pos_src = _resolve_place_of_supply(ledger)
    if pos:
        out["PLACEOFSUPPLY"] = pos
        sources["PLACEOFSUPPLY"] = pos_src

    pin, pin_src = _resolve_pincode(ledger)
    if pin:
        out["PINCODE"] = pin
        sources["PINCODE"] = pin_src

    mail, mail_src = _resolve_mailing_name(ledger)
    if mail:
        out["MAILINGNAME"] = mail
        sources["MAILINGNAME"] = mail_src

    cn, cn_src = _resolve_country(ledger)
    if cn:
        out["COUNTRY"] = cn
        out["COUNTRYNAME"] = _direct_text(ledger, "COUNTRYNAME") or cn
        sources["COUNTRY"] = cn_src

    if sources:
        out["field_sources"] = sources

    return out


def load_ledger_master_index(ledgers_xml: Path) -> dict[str, dict[str, Any]]:
    """Parse tally_ledgers_final.xml once; map LEDGER NAME -> detail dict."""
    index: dict[str, dict[str, Any]] = {}
    for _event, elem in ET.iterparse(str(ledgers_xml), events=("end",)):
        if elem.tag != "LEDGER":
            continue
        raw_name = (elem.attrib.get("NAME") or "").strip()
        if raw_name:
            index[raw_name] = extract_ledger_master_fields(elem)
        elem.clear()
    return index


def _element_to_obj(el: ET.Element) -> Any:
    """Convert an Element subtree to JSON-serializable dict / list / str / null."""
    children = list(el)
    attribs = {f"@{k}": v for k, v in el.attrib.items()}
    if not children:
        text = (el.text or "").strip()
        if attribs:
            if text:
                attribs["_text"] = text
            return attribs
        return text if text else None

    groups: dict[str, list[Any]] = defaultdict(list)
    for c in children:
        groups[c.tag].append(_element_to_obj(c))

    out: dict[str, Any] = dict(attribs)
    for tag, vals in groups.items():
        out[tag] = vals[0] if len(vals) == 1 else vals
    return out


def daybook_xml_to_json_structure(daybook_root: ET.Element) -> dict[str, Any]:
    if daybook_root.tag != "TALLYDAYBOOK":
        raise ValueError(f"Expected TALLYDAYBOOK root, got {daybook_root.tag!r}")
    return _element_to_obj(daybook_root)


def convert_one_voucher_file(
    voucher_xml: Path,
    ledger_index: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    stem = voucher_xml.stem
    ledger_master = ledger_index.get(stem)
    if ledger_master is None:
        ledger_master = {
            "NAME": stem,
            "_lookup_error": f"No <LEDGER NAME={stem!r}> in master file",
        }

    tree = ET.parse(voucher_xml)
    root = tree.getroot()
    daybook_json = daybook_xml_to_json_structure(root)

    return {
        "ledger_master": ledger_master,
        "daybook": daybook_json,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Convert vouchers_by_final_list XML files to JSON with ledger master details."
    )
    p.add_argument(
        "--ledgers",
        type=Path,
        default=Path(__file__).resolve().parent / "tally_ledgers_final.xml",
        help="Path to tally_ledgers_final.xml",
    )
    p.add_argument(
        "--vouchers-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "vouchers_by_final_list",
        help="Folder containing per-ledger TALLYDAYBOOK XML files",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "vouchers_by_final_list_json",
        help="Folder to write .json files (created if missing)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Load master and report counts only; do not write JSON",
    )
    args = p.parse_args(argv)

    if not args.ledgers.is_file():
        print(f"Ledger master not found: {args.ledgers}", file=sys.stderr)
        return 1
    if not args.vouchers_dir.is_dir():
        print(f"Vouchers folder not found: {args.vouchers_dir}", file=sys.stderr)
        return 1

    print(f"Loading {args.ledgers} ...", file=sys.stderr)
    ledger_index = load_ledger_master_index(args.ledgers)
    print(f"Indexed {len(ledger_index)} ledgers.", file=sys.stderr)

    xml_files = sorted(args.vouchers_dir.glob("*.xml"))
    if not xml_files:
        print(f"No XML files in {args.vouchers_dir}", file=sys.stderr)
        return 1

    missing_masters = 0
    for xf in xml_files:
        if xf.stem not in ledger_index:
            missing_masters += 1

    print(f"Found {len(xml_files)} voucher XML files.", file=sys.stderr)
    if missing_masters:
        print(
            f"Warning: {missing_masters} file stem(s) have no matching ledger NAME "
            f"(ledger_master will contain _lookup_error).",
            file=sys.stderr,
        )

    if args.dry_run:
        return 0

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for xf in xml_files:
        data = convert_one_voucher_file(xf, ledger_index)
        out_path = args.output_dir / f"{xf.stem}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")

    print(f"Wrote {len(xml_files)} JSON file(s) under {args.output_dir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
