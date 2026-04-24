"""
Export the complete Ledger Master from Tally over the XML HTTP API (localhost:9000).

Default CLI behaviour (full data):
  1. Loads the Group hierarchy and appends RESOLVED_ROOTPRIMARY / RESOLVED_NATURE /
     RESOLVED_FINANCIALSTATEMENT on each ledger (use --no-enrich to omit).
  2. Posts one Collection request with a wide <FETCH> so nested GST / mailing /
     statutory blocks are returned when Tally exposes them.
  3. Writes each <LEDGER> subtree as returned by Tally (deep-copied), preserving
     nested structures (e.g. LEDGSTREGDETAILS.LIST, GSTDETAILS.LIST).
     OPENINGBALANCE/CLOSINGBALANCE are removed from final output by design.
  3b. Ledgers whose NAME normalizes to the same string (whitespace-collapsed) are
       merged into one row: scalar tags use a text union (empty filled from the other;
       differing non-empty values joined with " | "); *.LIST blocks union child rows
       with duplicate subtrees skipped.
  4. By default, "beautifies" output: drops TYPE="..." attributes on tags, and when
     enrichment is on inserts ROOTPRIMARY / NATURE / FINANCIALSTATEMENT right after
     PARENT (same idea as tally_groups_final.xml). Use --no-beautify for raw Tally XML.

Legacy flattened export: --legacy-flat (no duplicate-name merge; native export merges).

Usage:
  python tally_ledger_master.py
  python tally_ledger_master.py --out my_ledgers.xml
  python tally_ledger_master.py --no-enrich
  python tally_ledger_master.py --no-beautify
  python tally_ledger_master.py --out flat.xml --legacy-flat

API:
  from tally_ledger_master import export_ledgers_to_path
  export_ledgers_to_path("tally_ledgers_final.xml")  # enrich + beautify on by default
"""

from __future__ import annotations

import copy
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from xml.etree.ElementTree import Element, ElementTree, SubElement, indent

import requests

# ---------------------------------------------------------------------------
# Ledger <FETCH> — wide list so Tally returns nested GST / registration / mailing
# blocks where the runtime supports them. If a request fails, remove unknown
# method names (Tally version–specific) until it succeeds.
# ---------------------------------------------------------------------------
LEDGER_FETCH = """
Name, Parent, GUID, MasterID, AlterID, ReservedName,
PriorStateName, PlaceOfSupply,
IsBillwiseOn, IsRevenue, IsDeemedPositive,
AffectsGrossProfit, IsSubledger, IsAddable,
IsCostCentresOn, IsCostTracking,
IsTDSApplicable, IsTCSApplicable,
IsGSTApplicable, GSTApplicable, GSTType,
GSTNatureOfSupply, GSTTypeOfSupply,
PartyGSTIN, PartyIdentification,
TaxType, TaxClassificationName,
IsTaxable, ExciseDutyApplicable,
GSTRegistrationType, TDSDeducteeType, ServiceTaxApplicable,
PriorGSTRegistrationType, ConsigneeGSTIN,
MailingName, Address, Address.List,
StateName, PINCode, CountryName, Country,
Email, Phone, Mobile, Fax, Website,
IncomeTaxNumber, PAN, GSTIN, TAN,
GSTDETAILS, HSNDETAILS,
LEDGSTREGDETAILS, LEDMAILINGDETAILS,
GSTREGISTRATIONDETAILS,
MSMEDetails, MSMEREGISTRATIONDETAILS,
ContactDetails, LedgerContact.List,
LEDMULTIADDRESSLIST, LEDADDRESS.LIST,
BankAccountNo, BankAccountNumber, BankName, IFSCCode,
MICRCode, BranchName, BankAccountType,
IsInterestActive, InterestType,
BaseUnits, CreditPeriod, CreditLimit,
CurrencyName, Narration,
LanguageName.List,
BillAllocations.List,
BillCreditPeriod,
LedgerFBTCategory, VATDealerType
""".replace("\n", " ")
LEDGER_FETCH = re.sub(r"\s+", " ", LEDGER_FETCH.strip())


def clean_tally_xml(text: str) -> str:
    """Sanitize Tally XML — fix illegal char refs, bare ampersands, and control chars."""

    def replace_dec(m: re.Match) -> str:
        n = int(m.group(1))
        if n in (9, 10, 13) or (0x20 <= n <= 0xD7FF):
            return m.group(0)
        return ""

    def replace_hex(m: re.Match) -> str:
        n = int(m.group(1), 16)
        if n in (9, 10, 13) or (0x20 <= n <= 0xD7FF):
            return m.group(0)
        return ""

    text = re.sub(r"&#(\d+);", replace_dec, text)
    text = re.sub(r"&#x([0-9a-fA-F]+);", replace_hex, text)
    text = re.sub(r"&(?!(amp;|lt;|gt;|quot;|apos;|#))", "&amp;", text)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
    return text


def post(xml: str) -> str:
    """Send a TDL XML request to Tally and return sanitized XML response."""
    r = requests.post(
        "http://localhost:9000",
        data=xml.encode("utf-8"),
        headers={"Content-Type": "text/xml"},
        timeout=600,
    )
    return clean_tally_xml(r.content.decode("utf-8", errors="replace"))


def norm_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def strip_type_attributes(elem: ET.Element) -> None:
    """Remove Tally TYPE='String' | 'Logical' | … attributes for cleaner, shorter XML."""
    keys_to_drop = [k for k in elem.attrib if k == "TYPE" or k.endswith("}TYPE")]
    for k in keys_to_drop:
        del elem.attrib[k]
    for child in elem:
        strip_type_attributes(child)




def strip_balance_tags(elem: ET.Element) -> None:
    """Remove OPENINGBALANCE/CLOSINGBALANCE from output ledger XML."""
    for child in list(elem):
        if child.tag in {"OPENINGBALANCE", "CLOSINGBALANCE"}:
            elem.remove(child)
        else:
            strip_balance_tags(child)


def _merge_text_union(a: str | None, b: str | None) -> str:
    """Combine two text values: prefer non-empty; if both set and differ, join with ' | '."""
    a = norm_text(a or "")
    b = norm_text(b or "")
    if not a:
        return b
    if not b:
        return a
    if a == b:
        return a
    return f"{a} | {b}"


def _merge_two_elements(dst: ET.Element, src: ET.Element) -> None:
    """Merge *src* into *dst* in-place so *dst* holds the union of both subtrees.

    - Leaf nodes: union text (see _merge_text_union).
    - Attributes: fill empty from *src*; if both non-empty and different, join like text.
    - Tags ending in ``.LIST`` (Tally list wrappers): union child rows — merge a single
      matching child by tag when unique; if several siblings share a tag, append *src*
      rows that are not already present (by whole-subtree string); identical rows drop.
    - Otherwise: for each child of *src*, append a deep copy if that tag is missing in
      *dst*; else merge into the first same-tag child (rare duplicate siblings).
    """
    if not len(list(src)) and not len(list(dst)):
        dst.text = _merge_text_union(dst.text, src.text)
        return

    for k, v in src.attrib.items():
        existing = dst.attrib.get(k, "")
        if not norm_text(str(existing)):
            if norm_text(str(v)):
                dst.attrib[k] = v
        elif norm_text(str(v)) and norm_text(str(existing)) != norm_text(str(v)):
            dst.attrib[k] = _merge_text_union(str(existing), str(v))

    dst.text = _merge_text_union(dst.text, src.text)

    # Tally *.LIST blocks: union rows (GST lines, address lists, etc.)
    if dst.tag.endswith(".LIST") and src.tag == dst.tag:
        for sch in list(src):
            same_tag = [ch for ch in dst if ch.tag == sch.tag]
            if len(same_tag) == 0:
                dst.append(copy.deepcopy(sch))
            elif len(same_tag) == 1:
                _merge_two_elements(same_tag[0], sch)
            else:
                fp = ET.tostring(sch, encoding="unicode")
                if not any(ET.tostring(x, encoding="unicode") == fp for x in same_tag):
                    dst.append(copy.deepcopy(sch))
        return

    for sch in list(src):
        matches = [ch for ch in dst if ch.tag == sch.tag]
        if not matches:
            dst.append(copy.deepcopy(sch))
        else:
            # Fold *sch* into the first same-tag sibling (duplicate tag siblings are rare).
            _merge_two_elements(matches[0], sch)


def _insert_after_tag(parent: ET.Element, after_tag: str, new_children: list[ET.Element]) -> None:
    """Insert elements immediately after the first child whose tag is *after_tag*."""
    ix: int | None = next(
        (i for i, ch in enumerate(parent) if ch.tag == after_tag),
        None,
    )
    if ix is None:
        for el in new_children:
            parent.append(el)
        return
    for j, el in enumerate(new_children):
        parent.insert(ix + 1 + j, el)


def _ledger_collection_envelope(fetch_spec: str) -> str:
    return f"""<ENVELOPE>
      <HEADER>
        <VERSION>1</VERSION>
        <TALLYREQUEST>Export</TALLYREQUEST>
        <TYPE>Collection</TYPE>
        <ID>LedgerFullDetails</ID>
      </HEADER>
      <BODY>
        <DESC>
          <STATICVARIABLES>
            <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
          </STATICVARIABLES>
          <TDL>
            <TDLMESSAGE>
              <COLLECTION NAME="LedgerFullDetails" ISMODIFY="No">
                <TYPE>Ledger</TYPE>
                <FETCH>{fetch_spec}</FETCH>
              </COLLECTION>
            </TDLMESSAGE>
          </TDL>
        </DESC>
      </BODY>
    </ENVELOPE>"""


PRIMARY_NATURE: dict[str, tuple[str, str]] = {
    "Capital Account": ("Liability", "Balance Sheet"),
    "Reserves & Surplus": ("Liability", "Balance Sheet"),
    "Loans (Liability)": ("Liability", "Balance Sheet"),
    "Current Liabilities": ("Liability", "Balance Sheet"),
    "Provisions": ("Liability", "Balance Sheet"),
    "Suspense A/c": ("Liability", "Balance Sheet"),
    "Branch / Divisions": ("Liability", "Balance Sheet"),
    "Expenses Payable": ("Liability", "Balance Sheet"),
    "Fixed Assets": ("Asset", "Balance Sheet"),
    "Current Assets": ("Asset", "Balance Sheet"),
    "Investments": ("Asset", "Balance Sheet"),
    "Loans & Advances (Asset)": ("Asset", "Balance Sheet"),
    "Misc. Expenses (ASSET)": ("Asset", "Balance Sheet"),
    "Deposits (Asset)": ("Asset", "Balance Sheet"),
    "Sales Accounts": ("Income", "P&L"),
    "Direct Incomes": ("Income", "P&L"),
    "Indirect Incomes": ("Income", "P&L"),
    "Purchase Accounts": ("Expense", "P&L"),
    "Direct Expenses": ("Expense", "P&L"),
    "Indirect Expenses": ("Expense", "P&L"),
    "Primary": ("Primary", "Root"),
}


def _get_root_primary(name: str, gmap: dict[str, str], depth: int = 0) -> str:
    if depth > 20:
        return name
    if name in PRIMARY_NATURE:
        return name
    parent = gmap.get(name, "")
    if not parent:
        return name
    return _get_root_primary(parent, gmap, depth + 1)


def _load_groups_map() -> dict[str, str]:
    groot = ET.fromstring(
        post(
            """<ENVELOPE>
      <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST>
      <TYPE>Collection</TYPE><ID>List of Groups</ID></HEADER>
      <BODY><DESC><STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
      <TDL><TDLMESSAGE>
        <COLLECTION NAME="List of Groups" ISMODIFY="No">
          <TYPE>Group</TYPE><FETCH>Name, Parent, IsRevenue</FETCH>
        </COLLECTION>
      </TDLMESSAGE></TDL></DESC></BODY></ENVELOPE>"""
        )
    )
    groups_map: dict[str, str] = {}
    for g in groot.findall(".//GROUP"):
        gn = norm_text(g.get("NAME"))
        if gn:
            groups_map[gn] = norm_text(g.findtext("PARENT", ""))
    return groups_map


def _export_flat(
    root: ET.Element,
    output_root: Element,
    groups_map: dict[str, str],
) -> tuple[int, int]:
    """Previous flattened schema (backward compatible)."""
    count = skipped = 0

    def txt(el: ET.Element, tag: str, default: str = "") -> str:
        v = el.findtext(tag, default)
        return norm_text(v) if v else default

    def get_list_values(el: ET.Element, list_tag: str, item_tag: str) -> list[str]:
        return [
            norm_text(n.text)
            for n in el.findall(f".//{list_tag}/{item_tag}")
            if norm_text(n.text or "")
        ]

    def bank_account_no(el: ET.Element) -> str:
        return txt(el, "BANKACCOUNTNUMBER") or txt(el, "BANKACCOUNTNO")

    for led in root.findall(".//LEDGER"):
        name = norm_text(led.get("NAME"))
        if not name:
            skipped += 1
            continue

        parent = txt(led, "PARENT")
        root_primary = _get_root_primary(parent, groups_map)
        nature, stmt = PRIMARY_NATURE.get(root_primary, ("Unknown", "Unknown"))

        address_parts = [
            norm_text(a.text)
            for a in led.findall(".//ADDRESS.LIST/ADDRESS")
            if norm_text(a.text or "")
        ]
        address = ", ".join(address_parts) if address_parts else txt(led, "ADDRESS")

        alt_names = get_list_values(led, "LANGUAGENAME.LIST", "NAME.LIST/NAME")
        alt_str = ", ".join(set(alt_names) - {name})

        pan = txt(led, "INCOMETAXNUMBER") or txt(led, "PAN")
        gstin = txt(led, "PARTYGSTIN") or txt(led, "GSTIN")
        country = txt(led, "COUNTRYNAME") or txt(led, "COUNTRY")

        l = SubElement(output_root, "LEDGER")

        def add(tag: str, val: str) -> None:
            e = SubElement(l, tag)
            e.text = val or ""

        add("NAME", name)
        add("GUID", txt(led, "GUID"))
        add("MASTERID", txt(led, "MASTERID"))
        add("ALTERID", txt(led, "ALTERID"))
        add("RESERVEDNAME", norm_text(led.get("RESERVEDNAME")))

        add("PARENT", parent)
        add("ROOTPRIMARY", root_primary)
        add("NATURE", nature)
        add("FINANCIALSTATEMENT", stmt)

        add("ISREVENUE", txt(led, "ISREVENUE"))
        add("ISDEEMEDPOSITIVE", txt(led, "ISDEEMEDPOSITIVE"))
        add("AFFECTSGROSSPROFIT", txt(led, "AFFECTSGROSSPROFIT"))
        add("ISBILLWISEON", txt(led, "ISBILLWISEON"))
        add("ISADDABLE", txt(led, "ISADDABLE"))
        add("ISSUBLEDGER", txt(led, "ISSUBLEDGER"))
        add("ISCOSTCENTRESON", txt(led, "ISCOSTCENTRESON"))
        add("ISCOSTTRACKING", txt(led, "ISCOSTTRACKING"))

        add("ISTDSAPPLICABLE", txt(led, "ISTDSAPPLICABLE"))
        add("ISTCSAPPLICABLE", txt(led, "ISTCSAPPLICABLE"))
        add("ISGSTAPPLICABLE", txt(led, "ISGSTAPPLICABLE"))
        add("GSTAPPLICABLE", txt(led, "GSTAPPLICABLE"))
        add("GSTTYPE", txt(led, "GSTTYPE"))
        add("GSTREGISTRATIONTYPE", txt(led, "GSTREGISTRATIONTYPE"))
        add("TAXTYPE", txt(led, "TAXTYPE"))
        add("TDSDEDUCTEETYPE", txt(led, "TDSDEDUCTEETYPE"))
        add("SERVICETAX", txt(led, "SERVICETAXAPPLICABLE"))
        add("TAXCLASSIFICATIONNAME", txt(led, "TAXCLASSIFICATIONNAME"))
        add("ISTAXABLE", txt(led, "ISTAXABLE"))
        add("EXCISEDUTYAPPLICABLE", txt(led, "EXCISEDUTYAPPLICABLE"))

        add("PAN", pan)
        add("GSTIN", gstin)
        add("TAN", txt(led, "TAN"))

        add("MAILINGNAME", txt(led, "MAILINGNAME"))
        add("ADDRESS", address)
        add("STATE", txt(led, "STATENAME"))
        add("PINCODE", txt(led, "PINCODE"))
        add("COUNTRY", country)
        add("EMAIL", txt(led, "EMAIL"))
        add("PHONE", txt(led, "PHONE"))
        add("MOBILE", txt(led, "MOBILE"))
        add("FAX", txt(led, "FAX"))
        add("WEBSITE", txt(led, "WEBSITE"))

        add("BANKACCOUNTNO", bank_account_no(led))
        add("BANKNAME", txt(led, "BANKNAME"))
        add("IFSCCODE", txt(led, "IFSCCODE"))
        add("MICRCODE", txt(led, "MICRCODE"))
        add("BANKACCOUNTTYPE", txt(led, "BANKACCOUNTTYPE"))
        add("BRANCHNAME", txt(led, "BRANCHNAME"))

        add("CREDITPERIOD", txt(led, "CREDITPERIOD"))
        add("CREDITLIMIT", txt(led, "CREDITLIMIT"))

        add("ISINTERESTACTIVE", txt(led, "ISINTERESTACTIVE"))
        add("INTERESTTYPE", txt(led, "INTERESTTYPE"))

        add("CURRENCY", txt(led, "CURRENCYNAME"))
        add("NARRATION", txt(led, "NARRATION"))
        add("ALTERNATENAMES", alt_str)

        count += 1

    return count, skipped


def export_ledgers_to_path(
    out_path: str | Path,
    *,
    enrich: bool = True,
    legacy_flat: bool = False,
    beautify: bool = True,
    fetch_spec: str | None = None,
) -> tuple[int, int]:
    """Fetch all ledgers from Tally and write XML to *out_path*.

    Parameters
    ----------
    enrich
        If True (default), resolve group hierarchy and add ROOTPRIMARY / NATURE /
        FINANCIALSTATEMENT after the ``PARENT`` tag (aligned with ``tally_groups_final``
        style). Only applies to **full** export, not --legacy-flat.
        After enrichment, ledgers with the same normalized ``NAME`` are merged into one
        record with unioned tags (see module docstring).
    legacy_flat
        If True, emit the old flattened schema (still uses the same wide FETCH for
        the Tally request so top-level GSTIN/PARTYGSTIN benefit).
    beautify
        If True (default), strip ``TYPE`` attributes from all elements in native mode.
        If False, leave Tally's attributes unchanged.
    fetch_spec
        Override the default LEDGER_FETCH string if your Tally build rejects some
        method names.

    Returns
        (count_exported, count_skipped)
    """
    out_path = Path(out_path)
    fetch = fetch_spec or LEDGER_FETCH

    groups_map: dict[str, str] = {}
    if legacy_flat or enrich:
        groups_map = _load_groups_map()

    xml_request = _ledger_collection_envelope(fetch)
    raw = post(xml_request)
    root = ET.fromstring(raw)

    output_root = Element("TALLYLEDGERS")
    if not legacy_flat:
        output_root.set("FORMAT", "tally_native_ledgers")
        if enrich:
            output_root.set("ENRICHED", "yes")
        if beautify:
            output_root.set("BEAUTIFIED", "yes")

    if legacy_flat:
        count, skipped = _export_flat(root, output_root, groups_map)
    else:
        count = skipped = 0
        # Group by normalized NAME so duplicate Tally ledgers (same name after norm_text)
        # are merged once below with _merge_two_elements (union of tags / LIST rows).
        buckets: dict[str, list[Element]] = defaultdict(list)
        for led in root.findall(".//LEDGER"):
            name_key = norm_text(led.get("NAME"))
            if not name_key:
                skipped += 1
                continue
            node = copy.deepcopy(led)
            if enrich:
                parent = norm_text(node.findtext("PARENT", ""))
                rp = _get_root_primary(parent, groups_map)
                nature, stmt = PRIMARY_NATURE.get(rp, ("Unknown", "Unknown"))
                extra = [
                    Element("ROOTPRIMARY"),
                    Element("NATURE"),
                    Element("FINANCIALSTATEMENT"),
                ]
                extra[0].text = rp
                extra[1].text = nature
                extra[2].text = stmt
                _insert_after_tag(node, "PARENT", extra)
            strip_balance_tags(node)
            buckets[name_key].append(node)

        for name_key, nodes in buckets.items():
            if len(nodes) == 1:
                merged = nodes[0]
            else:
                # First row is the base; each extra ledger is folded in so missing GST /
                # tax / address fields from any duplicate fill gaps (union semantics).
                merged = nodes[0]
                for extra in nodes[1:]:
                    _merge_two_elements(merged, extra)
            merged.set("NAME", name_key)
            if beautify:
                strip_type_attributes(merged)
            output_root.append(merged)
            count += 1

    output_root.set("TOTALCOUNT", str(count))
    output_root.set("SKIPPED", str(skipped))

    indent(output_root, space="  ")
    ElementTree(output_root).write(
        str(out_path),
        encoding="unicode",
        xml_declaration=True,
    )

    print(f"Total ledgers exported : {count}")
    print(f"Skipped (no name)      : {skipped}")
    print(f"Saved to               : {out_path}")
    return count, skipped


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(
        description="Export ledger master from Tally (native subtree or legacy flat)."
    )
    ap.add_argument(
        "--out",
        type=str,
        default="tally_ledgers_final.xml",
        help="Output XML path",
    )
    ap.add_argument(
        "--enrich",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Append RESOLVED_* group classification (default: on). "
            "Use --no-enrich for Tally-only tags without group resolution."
        ),
    )
    ap.add_argument(
        "--legacy-flat",
        action="store_true",
        help="Emit the old flattened <LEDGER> schema instead of copying Tally's subtree.",
    )
    ap.add_argument(
        "--beautify",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Strip TYPE attributes on tags and keep output readable (default: on). "
            "Use --no-beautify for raw Tally attributes."
        ),
    )
    args = ap.parse_args()
    export_ledgers_to_path(
        args.out,
        enrich=args.enrich,
        legacy_flat=args.legacy_flat,
        beautify=args.beautify,
    )


if __name__ == "__main__":
    main()
