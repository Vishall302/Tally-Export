"""
Export the complete Group hierarchy from Tally and classify each group by nature.

Tally organizes all ledgers into a tree of Groups. Each group ultimately traces
back to one of ~20 fixed "primary" groups (e.g. Current Assets, Sales Accounts).
This script:
  1. Fetches the full group list from Tally via HTTP XML API (localhost:9000).
  2. Maps each group to its root primary ancestor via parent-chain traversal.
  3. Classifies each group as Asset/Liability/Income/Expense and Balance Sheet/P&L.
  4. Writes the enriched hierarchy to tally_groups_final.xml.

Library use: call ``export_groups_to_path(out_path)`` — importing this module no
longer triggers a live Tally request (the fetch/build/write is inside the function).
Running the file directly still writes ``tally_groups_final.xml`` in the CWD.
"""

import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, ElementTree, indent
from xml.sax.saxutils import escape as _xml_escape
import re


def _company_var(company):
    """An ``<SVCURRENTCOMPANY>`` line pinning the request to *company*, else ''.

    Scopes the Group collection to one company so a multi-company Tally session
    can't return another company's groups. The name is XML-escaped.
    """
    if not company or not company.strip():
        return ""
    return f"<SVCURRENTCOMPANY>{_xml_escape(company.strip())}</SVCURRENTCOMPANY>"

# ── TDL XML request: fetch all Group attributes from Tally ────────────────────
XML_REQUEST = """<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export</TALLYREQUEST>
    <TYPE>Collection</TYPE>
    <ID>List of Groups</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      </STATICVARIABLES>
      <TDL>
        <TDLMESSAGE>
          <COLLECTION NAME="List of Groups" ISMODIFY="No">
            <TYPE>Group</TYPE>
            <FETCH>
              Name, Parent, GUID, MasterID, AlterID,
              IsRevenue, IsDeemedPositive, AffectsGrossProfit,
              IsBillwiseOn, IsAddable, IsSubledger,
              IsCostCentresOn, AddlAllocType,
              LanguageName.List
            </FETCH>
          </COLLECTION>
        </TDLMESSAGE>
      </TDL>
    </DESC>
  </BODY>
</ENVELOPE>"""

def clean_tally_xml(text):
    """Sanitize Tally XML — fix illegal char refs, bare ampersands, and control chars.
    See tally_daybook.clean_tally_xml for the full explanation.
    """
    def replace_dec(m):
        n = int(m.group(1))
        if n in (9, 10, 13) or (0x20 <= n <= 0xD7FF):
            return m.group(0)
        return ''
    def replace_hex(m):
        n = int(m.group(1), 16)
        if n in (9, 10, 13) or (0x20 <= n <= 0xD7FF):
            return m.group(0)
        return ''
    text = re.sub(r'&#(\d+);', replace_dec, text)
    text = re.sub(r'&#x([0-9a-fA-F]+);', replace_hex, text)
    text = re.sub(r'&(?!(amp;|lt;|gt;|quot;|apos;|#))', '&amp;', text)
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', text)
    return text

def norm_text(value: str | None) -> str:
    """Normalize internal whitespace to keep names stable for matching."""
    return re.sub(r"\s+", " ", value or "").strip()


# ── Build a flat dict of all groups from parsed XML ──────────────────
def _build_groups_raw(root):
    groups_raw = {}
    for grp in root.findall(".//GROUP"):
        name = norm_text(grp.get("NAME"))
        if not name:
            continue
        groups_raw[name] = {
            "parent":            norm_text(grp.findtext("PARENT", "")),
            "guid":              norm_text(grp.findtext("GUID", "")),
            "masterid":          norm_text(grp.findtext("MASTERID", "")),
            "alterid":           norm_text(grp.findtext("ALTERID", "")),
            "reservedname":      norm_text(grp.get("RESERVEDNAME")),
            "isrevenue":         norm_text(grp.findtext("ISREVENUE", "")),
            "isdeemedpositive":  norm_text(grp.findtext("ISDEEMEDPOSITIVE", "")),
            "affectsgp":         norm_text(grp.findtext("AFFECTSGROSSPROFIT", "")),
            "isbillwiseon":      norm_text(grp.findtext("ISBILLWISEON", "")),
            "isaddable":         norm_text(grp.findtext("ISADDABLE", "")),
            "issubledger":       norm_text(grp.findtext("ISSUBLEDGER", "")),
            "iscostcentreson":   norm_text(grp.findtext("ISCOSTCENTRESON", "")),
            "addlalloctype":     norm_text(grp.findtext("ADDLALLOCTYPE", "")),
            "alternatenames":    ", ".join(
                norm_text(n.text)
                for n in grp.findall(".//LANGUAGENAME.LIST/NAME.LIST/NAME")
                if norm_text(n.text)
            ),
        }
    return groups_raw


# ── Step 2: Tally's fixed primary groups → nature mapping ─────────────
# Tally has ~20 built-in "primary" groups that never change across any company.
# Each maps to a financial nature (Asset/Liability/Income/Expense) and a
# financial statement (Balance Sheet or P&L). User-created groups inherit
# their nature from their root primary ancestor.
PRIMARY_NATURE = {
    "Capital Account":           ("Liability", "Balance Sheet"),
    "Reserves & Surplus":        ("Liability", "Balance Sheet"),
    "Loans (Liability)":         ("Liability", "Balance Sheet"),
    "Current Liabilities":       ("Liability", "Balance Sheet"),
    "Provisions":                ("Liability", "Balance Sheet"),
    "Suspense A/c":              ("Liability", "Balance Sheet"),
    "Branch / Divisions":        ("Liability", "Balance Sheet"),
    "Expenses Payable":          ("Liability", "Balance Sheet"),
    "Fixed Assets":              ("Asset",     "Balance Sheet"),
    "Current Assets":            ("Asset",     "Balance Sheet"),
    "Investments":               ("Asset",     "Balance Sheet"),
    "Loans & Advances (Asset)":  ("Asset",     "Balance Sheet"),
    "Misc. Expenses (ASSET)":    ("Asset",     "Balance Sheet"),
    "Deposits (Asset)":          ("Asset",     "Balance Sheet"),
    "Sales Accounts":            ("Income",    "P&L"),
    "Direct Incomes":            ("Income",    "P&L"),
    "Indirect Incomes":          ("Income",    "P&L"),
    "Purchase Accounts":         ("Expense",   "P&L"),
    "Direct Expenses":           ("Expense",   "P&L"),
    "Indirect Expenses":         ("Expense",   "P&L"),
    "Primary":                   ("Primary",   "Root"),
}

# ── Step 3: Walk parent chain to find root primary group ──────────────
def get_root_primary(name, groups_dict, depth=0):
    """Recursively walk up to find the Tally primary group ancestor."""
    if depth > 20:  # prevent infinite loop
        return name
    if name in PRIMARY_NATURE:
        return name
    info = groups_dict.get(name)
    if not info or not info["parent"]:
        return name
    return get_root_primary(info["parent"], groups_dict, depth + 1)

# ── Fetch from Tally + build output XML ───────────────────────────────
def export_groups_to_path(
    out_path: str | Path = "tally_groups_final.xml",
    *,
    tally_url: str = "http://localhost:9000",
    timeout: int = 600,
    company: str | None = None,
) -> int:
    """Fetch the Group hierarchy from Tally, classify each group, and write XML.

    Returns the number of groups exported. Safe to import — the live Tally HTTP
    request happens only when this function is called.

    When *company* is given, the request is pinned to it via ``<SVCURRENTCOMPANY>``
    so groups from other loaded companies are never returned.
    """
    request = XML_REQUEST
    cv = _company_var(company)
    if cv:
        request = request.replace(
            "<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>",
            f"<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>\n        {cv}",
        )
    resp = requests.post(
        tally_url,
        data=request.encode("utf-8"),
        headers={"Content-Type": "text/xml"},
        timeout=timeout,
    )
    raw = resp.content.decode("utf-8", errors="replace")
    root = ET.fromstring(clean_tally_xml(raw))
    groups_raw = _build_groups_raw(root)

    output_root = Element("TALLYGROUPS")

    for name, info in sorted(groups_raw.items()):
        root_primary = get_root_primary(name, groups_raw)
        nature, statement = PRIMARY_NATURE.get(root_primary, ("Unknown", "Unknown"))

        g = SubElement(output_root, "GROUP")

        def add(tag, text):
            """Helper: append a child element with text to the current group node."""
            el = SubElement(g, tag)
            el.text = text or ""

        # Identity
        add("NAME",             name)
        add("RESERVEDNAME",     info["reservedname"])
        add("GUID",             info["guid"])
        add("MASTERID",         info["masterid"])
        add("ALTERID",          info["alterid"])

        # Hierarchy
        add("PARENT",           info["parent"])
        add("ROOTPRIMARY",      root_primary)       # e.g. "Indirect Expenses"
        add("NATURE",           nature)             # Asset/Liability/Income/Expense
        add("FINANCIALSTATEMENT", statement)        # Balance Sheet / P&L / Root

        # Behaviour
        add("ISREVENUE",          info["isrevenue"])
        add("ISDEEMEDPOSITIVE",   info["isdeemedpositive"])
        add("AFFECTSGROSSPROFIT", info["affectsgp"])
        add("ISBILLWISEON",       info["isbillwiseon"])
        add("ISADDABLE",          info["isaddable"])
        add("ISSUBLEDGER",        info["issubledger"])
        add("ISCOSTCENTRESON",    info["iscostcentreson"])
        add("ADDLALLOCTYPE",      info["addlalloctype"])
        add("ALTERNATENAMES",     info["alternatenames"])

    output_root.set("TOTALCOUNT", str(len(groups_raw)))

    indent(output_root, space="  ")
    ElementTree(output_root).write(
        str(out_path),
        encoding="unicode",
        xml_declaration=True,
    )
    print(f"Total groups exported: {len(groups_raw)}")
    print(f"Saved to: {out_path}")
    return len(groups_raw)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Export group hierarchy from Tally.")
    ap.add_argument("--out", default="tally_groups_final.xml", help="Output XML path")
    ap.add_argument(
        "--company",
        default=None,
        help="Pin the export to this Tally company (avoids mixing when several are open).",
    )
    a = ap.parse_args()
    export_groups_to_path(a.out, company=a.company)
