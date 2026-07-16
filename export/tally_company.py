"""List / resolve the Tally company for CLI exports over the XML HTTP API.

Standalone helper for ``run.py`` (the webapp has its own copy in
``webapp/backend/tally_pipeline.py``). Lets the CLI pin every export to one
company so a multi-company Tally session can't mix another company's vouchers
into the audit.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET

import requests

_TALLY_URL = "http://localhost:9000"

# "List of Companies" collection — returns a <NAME> per loaded company.
_LIST_XML = (
    "<ENVELOPE><HEADER><VERSION>1</VERSION>"
    "<TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE>"
    "<ID>List of Companies</ID></HEADER>"
    "<BODY><DESC><STATICVARIABLES>"
    "<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>"
    "</STATICVARIABLES></DESC></BODY></ENVELOPE>"
)
# $$CurrentCompany Function — the *active* company even when several are open.
_CURRENT_XML = (
    "<ENVELOPE><HEADER><VERSION>1</VERSION>"
    "<TALLYREQUEST>Export</TALLYREQUEST><TYPE>Function</TYPE>"
    "<ID>$$CurrentCompany</ID></HEADER>"
    "<BODY><DESC></DESC></BODY></ENVELOPE>"
)


def _post(xml: str, timeout: float = 5.0) -> str | None:
    try:
        r = requests.post(
            _TALLY_URL,
            data=xml.encode("utf-8"),
            headers={"Content-Type": "text/xml"},
            timeout=timeout,
        )
        if r.status_code != 200:
            return None
        # Reuse the export sanitizer — Tally emits bare ampersands / illegal
        # char refs that break ElementTree otherwise.
        from export.tally_ledger_master import clean_tally_xml
        return clean_tally_xml(r.content.decode("utf-8", errors="replace"))
    except requests.RequestException:
        return None


def _norm(value: str | None) -> str | None:
    name = " ".join((value or "").split())
    return name or None


def get_current_company(timeout: float = 5.0) -> str | None:
    """Name of the active company in Tally, or None. Never raises."""
    text = _post(_CURRENT_XML, timeout)
    if text:
        try:
            name = _norm(ET.fromstring(text).findtext(".//RESULT"))
            if name:
                return name
        except ET.ParseError:
            pass
    return None


def list_open_companies(timeout: float = 5.0) -> list[dict]:
    """All companies currently loaded in Tally, active one flagged. [] if unreachable.

    Returns ``[{"name": str, "active": bool}, ...]``. Never raises.
    """
    text = _post(_LIST_XML, timeout)
    if not text:
        return []
    names: list[str] = []
    seen: set[str] = set()
    try:
        for el in ET.fromstring(text).iter("NAME"):
            n = _norm(el.text)
            if n and n not in seen:
                seen.add(n)
                names.append(n)
    except ET.ParseError:
        return []
    active = get_current_company(timeout)
    return [{"name": n, "active": n == active} for n in names]


def resolve_company_interactive(explicit: str | None = None) -> str | None:
    """Resolve which company to export for the CLI.

    ``explicit`` (``--company``) wins. Otherwise: one open → use it; several open
    → print a numbered list and prompt; none listable → None (export unpinned).
    """
    if explicit:
        return explicit
    companies = list_open_companies()
    if not companies:
        return None
    if len(companies) == 1:
        return companies[0]["name"]
    print("\nSeveral companies are open in Tally. Pick the one to audit:")
    for i, c in enumerate(companies, 1):
        mark = "  (active)" if c["active"] else ""
        print(f"  {i}. {c['name']}{mark}")
    while True:
        choice = input(f"Enter number [1-{len(companies)}]: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(companies):
            return companies[int(choice) - 1]["name"]
        print("  Invalid choice, try again.")
