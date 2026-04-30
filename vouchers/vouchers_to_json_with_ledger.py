#!/usr/bin/env python3
"""
Build per-ledger JSON payloads from Tally daybook XML exports.

Inputs
------
1) ``tally_ledgers_final.xml`` (master ledger dump)
   - Contains ``<LEDGER>`` records used for static details:
     group hierarchy, tax identity fields, address/state, etc.
2) ``vouchers_by_final_list/*.xml``
   - Per-ledger ``<TALLYDAYBOOK>`` files produced earlier in your pipeline.
   - Filename stem is treated as ledger key.

Working (step by step)
----------------------
1) Parse master XML once and build an in-memory index:
   ``LEDGER NAME -> flattened ledger_master fields``.
2) Scan all voucher XML files in ``vouchers_by_final_list``.
3) For each voucher file:
   - derive ledger key from filename stem;
   - fetch matching master ledger from index;
   - if missing, keep processing and attach ``_lookup_error``;
   - parse ``<TALLYDAYBOOK>`` and recursively convert XML to JSON objects.
4) Merge both sections into one payload:
   ``{"ledger_master": ..., "daybook": ...}``.
5) Write one JSON file per voucher file in output directory.

How ledger matching works
-------------------------
The voucher filename stem must equal the ``NAME`` attribute of a ``<LEDGER>`` node in
the master file. Example: ``A.K. Associates.xml`` -> lookup ``<LEDGER NAME="A.K. Associates">``.
If no match is found, output still gets written with a ``_lookup_error`` marker under
``ledger_master`` so downstream processing can detect missing master rows.

Field normalization behavior
----------------------------
Tally can store the same business fact in multiple places (for example GSTIN in
``PARTYGSTIN`` and in repeated ``LEDGSTREGDETAILS.LIST/GSTIN`` blocks). This script:
- chooses one canonical value per concept (GSTIN, PAN, state, etc.) using fixed
  preference order;
- preserves traceability in ``field_sources`` (which tag supplied the chosen value);
- records ``*_all_distinct`` arrays when conflicting non-empty values are present.

Output
------
For each input voucher XML, output JSON contains:
- ``ledger_master``: flattened master metadata for that ledger;
- ``daybook``: full ``<TALLYDAYBOOK>`` converted recursively to JSON-friendly objects.

Output files are written to ``vouchers_by_final_list_json/`` by default, using the same
base filename with ``.json`` extension. The script uses only Python standard library
modules and supports ``--dry-run`` for validation without writing files.

CLI usage
---------
- Default run:
  ``python vouchers_to_json_with_ledger.py``
- Custom paths:
  ``python vouchers_to_json_with_ledger.py --ledgers tally_ledgers_final.xml --vouchers-dir vouchers_by_final_list --output-dir vouchers_by_final_list_json``
- Validation only (no write):
  ``python vouchers_to_json_with_ledger.py --dry-run``
"""

from __future__ import annotations

import argparse
import io
import json
import re
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
    # Preference order matters: the first non-empty value wins.
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

    # Collect all GSTIN candidates and pick a canonical value while retaining provenance.
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

    # PAN can be repeated across nested blocks in Tally exports.
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


def _sanitize_xml_for_iterparse(raw: bytes) -> bytes:
    """
    Best-effort cleanup for malformed Tally exports before XML parsing.
    - Fixes bad opening tags like <GSTTYPEOFSUPPLY'> -> <GSTTYPEOFSUPPLY>
    - Removes control bytes disallowed by XML 1.0
    """
    cleaned = re.sub(rb"<([A-Za-z_][\w.\-:]*)'>", rb"<\1>", raw)
    return re.sub(rb"[\x00-\x08\x0B\x0C\x0E-\x1F]", b"", cleaned)


def _normalize_ledger_key(name: str) -> str:
    """
    Normalize ledger names for tolerant matching across filename/master variants.
    Examples handled: M_S <-> M/S, A_c <-> A/C, punctuation/space differences.
    """
    s = name.strip().lower()
    s = s.replace("&amp;", "&")
    s = s.replace("m/s", "ms").replace("m_s", "ms")
    s = s.replace("a/c", "ac").replace("a_c", "ac")
    return re.sub(r"[^a-z0-9]+", "", s)


def _build_normalized_ledger_index(
    ledger_index: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """
    Build normalized key -> master row map. First seen wins on key collisions.
    """
    out: dict[str, dict[str, Any]] = {}
    for raw_name, details in ledger_index.items():
        key = _normalize_ledger_key(raw_name)
        if key and key not in out:
            out[key] = details
    return out


def _lookup_ledger_master(
    ledger_name: str,
    ledger_index: dict[str, dict[str, Any]],
    normalized_ledger_index: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    exact = ledger_index.get(ledger_name)
    if exact is not None:
        return exact
    return normalized_ledger_index.get(_normalize_ledger_key(ledger_name))


def load_ledger_master_index(ledgers_xml: Path) -> dict[str, dict[str, Any]]:
    """Parse tally_ledgers_final.xml once; map LEDGER NAME -> detail dict."""
    index: dict[str, dict[str, Any]] = {}

    def _consume(source: Any) -> None:
        for _event, elem in ET.iterparse(source, events=("end",)):
            if elem.tag != "LEDGER":
                continue
            raw_name = (elem.attrib.get("NAME") or "").strip()
            if raw_name:
                index[raw_name] = extract_ledger_master_fields(elem)
            elem.clear()

    try:
        _consume(str(ledgers_xml))
    except ET.ParseError as exc:
        # Some Tally exports contain malformed tokens; sanitize and retry so processing continues.
        print(
            f"Warning: malformed XML in {ledgers_xml} ({exc}); retrying with sanitization.",
            file=sys.stderr,
        )
        sanitized = _sanitize_xml_for_iterparse(ledgers_xml.read_bytes())
        _consume(io.BytesIO(sanitized))
    return index


def _element_to_obj(el: ET.Element) -> Any:
    """Convert an Element subtree to JSON-serializable dict / list / str / null."""
    # Conversion policy:
    # - leaf node => text (or null if blank)
    # - attributes are kept with "@attr" keys
    # - when a node has both attributes and text, text is stored under "_text"
    # - repeated child tags become lists; singleton tags stay scalar objects
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
    # Guard against unexpected XML shape early so downstream output is predictable.
    if daybook_root.tag != "TALLYDAYBOOK":
        raise ValueError(f"Expected TALLYDAYBOOK root, got {daybook_root.tag!r}")
    return _element_to_obj(daybook_root)


def _inject_ledger_type_in_entries(
    node: Any,
    ledger_index: dict[str, dict[str, Any]],
    normalized_ledger_index: dict[str, dict[str, Any]],
) -> None:
    """
    Walk the daybook JSON tree and add Ledger_type to ledger-entry objects.
    Any object with LEDGERNAME is treated as an entry candidate.
    """
    if isinstance(node, dict):
        ledger_name = node.get("LEDGERNAME")
        if isinstance(ledger_name, str) and ledger_name.strip():
            master = _lookup_ledger_master(
                ledger_name.strip(), ledger_index, normalized_ledger_index
            )
            nature = ""
            if master is not None:
                raw_nature = master.get("NATURE")
                if isinstance(raw_nature, str):
                    nature = raw_nature
            node["Ledger_type"] = nature
        for value in node.values():
            _inject_ledger_type_in_entries(value, ledger_index, normalized_ledger_index)
        return

    if isinstance(node, list):
        for item in node:
            _inject_ledger_type_in_entries(item, ledger_index, normalized_ledger_index)


def convert_one_voucher_file(
    voucher_xml: Path,
    ledger_index: dict[str, dict[str, Any]],
    normalized_ledger_index: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    # File stem is expected to match <LEDGER NAME="..."> in master XML.
    stem = voucher_xml.stem
    ledger_master = _lookup_ledger_master(stem, ledger_index, normalized_ledger_index)
    if ledger_master is None:
        ledger_master = {
            "NAME": stem,
            "_lookup_error": f"No <LEDGER NAME={stem!r}> in master file",
        }

    # Parse the per-ledger daybook XML and serialize it into JSON-friendly primitives.
    tree = ET.parse(voucher_xml)
    root = tree.getroot()
    daybook_json = daybook_xml_to_json_structure(root)
    _inject_ledger_type_in_entries(daybook_json, ledger_index, normalized_ledger_index)

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

    # Build an in-memory NAME -> master-details index once, then reuse for all files.
    print(f"Loading {args.ledgers} ...", file=sys.stderr)
    ledger_index = load_ledger_master_index(args.ledgers)
    # Required for tolerant lookups when voucher filename stem differs by punctuation/casing.
    normalized_ledger_index = _build_normalized_ledger_index(ledger_index)
    print(f"Indexed {len(ledger_index)} ledgers.", file=sys.stderr)

    xml_files = sorted(args.vouchers_dir.glob("*.xml"))
    if not xml_files:
        print(f"No XML files in {args.vouchers_dir}", file=sys.stderr)
        return 1

    # Pre-compute missing NAME matches to surface quality issues before writing outputs.
    missing_masters = 0
    for xf in xml_files:
        if _lookup_ledger_master(xf.stem, ledger_index, normalized_ledger_index) is None:
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

    # Convert each XML file independently; one output JSON per input stem.
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for xf in xml_files:
        data = convert_one_voucher_file(xf, ledger_index, normalized_ledger_index)
        out_path = args.output_dir / f"{xf.stem}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")

    print(f"Wrote {len(xml_files)} JSON file(s) under {args.output_dir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
