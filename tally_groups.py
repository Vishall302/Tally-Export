"""
Export the complete Group hierarchy from Tally and classify each group by nature.

Tally organizes all ledgers into a tree of Groups. Each group ultimately traces
back to one of ~20 fixed "primary" groups (e.g. Current Assets, Sales Accounts).
This script:
  1. Fetches the full group list from Tally via HTTP XML API (localhost:9000).
  2. Maps each group to its root primary ancestor via parent-chain traversal.
  3. Classifies each group as Asset/Liability/Income/Expense and Balance Sheet/P&L.
  4. Writes the enriched hierarchy to tally_groups_final.xml.

NOTE: This script executes immediately at module level (not guarded by __main__).
Importing this module will trigger a live Tally HTTP request. For library use,
consider refactoring the execution into a main() function.
"""

import requests
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element, SubElement, ElementTree, indent
import re

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

# ── Step 0: Fetch raw group XML from Tally ────────────────────────────────────
resp = requests.post(
    "http://localhost:9000",
    data=XML_REQUEST.encode("utf-8"),
    headers={"Content-Type": "text/xml"}
)

raw = resp.content.decode("utf-8", errors="replace")

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

cleaned = clean_tally_xml(raw)
root = ET.fromstring(cleaned)


def norm_text(value: str | None) -> str:
    """Normalize internal whitespace to keep names stable for matching."""
    return re.sub(r"\s+", " ", value or "").strip()


# ── Step 1: Build a flat dict of all groups from XML ──────────────────
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

# ── Step 4: Build output XML ──────────────────────────────────────────
output_root = Element("TALLYGROUPS")
skipped = 0

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
    "tally_groups_final.xml",
    encoding="unicode",
    xml_declaration=True
)
print(f"Total groups exported: {len(groups_raw)}")
print(f"Saved to: tally_groups_final.xml")
