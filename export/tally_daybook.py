"""
Export the complete Daybook (voucher register) from Tally via its XML HTTP API.

Tally exposes an HTTP server on localhost:9000 that accepts TDL XML collection
requests and returns raw XML responses. This module:
  1. Builds a TDL request envelope asking for all voucher fields.
  2. Fetches data month-by-month to avoid Tally read-timeout on large date ranges.
  3. Deduplicates vouchers by GUID across overlapping chunks.
  4. Normalizes the response into a clean, consistent XML structure.

Usage:
  CLI:  python tally_daybook.py --start 01-04-2024 --end 31-03-2025
  API:  from tally_daybook import export_daybook_to_path
"""

import argparse
import requests
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element, SubElement, ElementTree, indent
from xml.sax.saxutils import escape as _xml_escape
import re
from calendar import monthrange
from collections import Counter
from datetime import datetime
from pathlib import Path

# Connect timeout (seconds), read timeout (seconds).
# Tally can be very slow for large date ranges — a single year-long export
# often exceeds 300s, hence the generous 900s read timeout.
DEFAULT_POST_TIMEOUT = (30, 900)


def clean_tally_xml(text):
    """Sanitize raw XML from Tally so it can be parsed by Python's ElementTree.

    Tally's XML output has several non-standard issues:
      - Illegal XML character references (&#0; through &#8;, etc.)
      - Unescaped ampersands in ledger names and narrations
      - Control characters (0x00-0x1F) embedded in text nodes
      - Namespace prefixes and xmlns declarations that confuse ElementTree
    This function strips/fixes all of these before parsing.
    """
    # Remove illegal decimal character references (keep tab, newline, CR, and valid Unicode)
    def replace_dec(m):
        n = int(m.group(1))
        if n in (9, 10, 13) or (0x20 <= n <= 0xD7FF):
            return m.group(0)
        return ''
    # Same for hex character references
    def replace_hex(m):
        n = int(m.group(1), 16)
        if n in (9, 10, 13) or (0x20 <= n <= 0xD7FF):
            return m.group(0)
        return ''
    text = re.sub(r'&#(\d+);', replace_dec, text)
    text = re.sub(r'&#x([0-9a-fA-F]+);', replace_hex, text)
    # Escape bare ampersands (but not already-valid XML entities like &amp; &lt; etc.)
    text = re.sub(r'&(?!(amp;|lt;|gt;|quot;|apos;|#))', '&amp;', text)
    # Strip ASCII control characters that are invalid in XML 1.0
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', text)
    # Remove namespace prefixes (e.g. <ns:TAG> -> <TAG>) so ElementTree can find tags
    text = re.sub(r'<(/?)([A-Za-z0-9_]+):([A-Za-z0-9_.\-]+)', r'<\1\3', text)
    # Strip xmlns declarations entirely
    text = re.sub(r'\s+xmlns[^"]*"[^"]*"', '', text)
    text = re.sub(r"\s+xmlns[^']*'[^']*'", '', text)
    return text


def post(xml, timeout=DEFAULT_POST_TIMEOUT):
    """Send a TDL XML request to Tally's HTTP server and return sanitized XML response."""
    r = requests.post(
        "http://localhost:9000",
        data=xml.encode("utf-8"),
        headers={"Content-Type": "text/xml"},
        timeout=timeout,
    )
    r.raise_for_status()
    return clean_tally_xml(r.content.decode("utf-8", errors="replace"))


def to_tally_date(d):
    """Convert DD-MM-YYYY string to Tally's expected DD-Mon-YYYY format (e.g. 01-Apr-2024)."""
    return datetime.strptime(d, "%d-%m-%Y").strftime("%d-%b-%Y")


def dt_to_tally(d):
    """Convert a datetime object to Tally's DD-Mon-YYYY format."""
    return d.strftime("%d-%b-%Y")


def iter_month_chunks(start_dt, end_dt):
    """Yield (chunk_start, chunk_end) datetime (date-only, start of day) for each month in range."""
    y, m = start_dt.year, start_dt.month
    end_key = (end_dt.year, end_dt.month)
    while (y, m) <= end_key:
        last_day = monthrange(y, m)[1]
        first = datetime(y, m, 1)
        last = datetime(y, m, last_day)
        chunk_start = max(start_dt, first)
        chunk_end = min(end_dt, last)
        yield chunk_start, chunk_end
        if m == 12:
            m, y = 1, y + 1
        else:
            m += 1


def _company_var(company):
    """An ``<SVCURRENTCOMPANY>`` line pinning the request to *company*, else ''.

    Without it, a Voucher collection on a Tally instance with several companies
    loaded returns vouchers from *all* of them — polluting a client's audit with
    another company's transactions. The name is XML-escaped (names may contain ``&``).
    """
    if not company or not company.strip():
        return ""
    return f"<SVCURRENTCOMPANY>{_xml_escape(company.strip())}</SVCURRENTCOMPANY>"


def _company_guid(v):
    """Company-GUID prefix of a voucher: its GUID/VCHKEY minus the trailing object id.

    Tally object GUIDs look like ``<company-uuid>-<objectid>`` (e.g.
    ``1b099cc1-d315-4231-ba62-509aea26d4fb-0000281c``). Every object in one company
    shares the ``<company-uuid>`` prefix, so it identifies the owning company.
    Returns '' when no GUID is available.
    """
    raw = txt(v, "GUID") or (v.get("VCHKEY") or "").strip()
    if not raw or "-" not in raw:
        return ""
    return raw.rsplit("-", 1)[0]


def envelope_daybook(start_tally, end_tally, company=None):
    """Build the TDL XML envelope that requests all daybook vouchers from Tally.

    The FETCH list includes every field needed for a complete voucher export:
    header fields, GST/tax details, status flags, ledger entries (with GST rate
    details, bill allocations, bank allocations, TDS), and inventory entries.

    When *company* is given, the request is pinned to it via ``<SVCURRENTCOMPANY>``
    so vouchers from other loaded companies are never returned.
    """
    return f"""<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export</TALLYREQUEST>
    <TYPE>Collection</TYPE>
    <ID>DayBookFull</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
        {_company_var(company)}
        <SVFROMDATE TYPE="Date">{start_tally}</SVFROMDATE>
        <SVTODATE TYPE="Date">{end_tally}</SVTODATE>
        <SVPERIODTYPE>Vouchers</SVPERIODTYPE>
      </STATICVARIABLES>
      <TDL>
        <TDLMESSAGE>
          <COLLECTION NAME="DayBookFull" ISMODIFY="No">
            <TYPE>Voucher</TYPE>
            <FETCH>
              Date, EffectiveDate, GUID, MasterID, AlterID,
              VoucherTypeName, VoucherNumber, VoucherNumberSeries,
              NumberingStyle, ReferenceNumber, ReferenceDate,
              PartyledgerName, PartyMailingName, Amount, Narration,
              IsOptional, IsCancelled, IsDeleted, IsInvoice,
              IsDeemedPositive, IsNegIsPosSet, AsOriginal,
              PersistedView, VoucherKey,
              GSTRegistrationType, PlaceOfSupply,
              Reference, CostCentreName,
              VoucherRetainKey, AsPaySlip,
              IsDeletedVchRetained,
              PartyGSTIN, ConsigneeGSTIN,
              StateName, ConsigneeStateName,
              CMPGSTState, CMPGSTIN,
              GSTRegistration, CMPGSTRegistrationType,
              BasicBuyerName, BasicDateTimeOfInvoice,
              IsEcommerceSupply, IsReverseChargeApplicable,
              AllLedgerEntries.List,
              AllinventoryEntries.List
            </FETCH>
          </COLLECTION>
        </TDLMESSAGE>
      </TDL>
    </DESC>
  </BODY>
</ENVELOPE>"""

def fmt_date(d):
    """Convert Tally's YYYYMMDD date string to YYYY-MM-DD format for readability."""
    if d and len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d or ""

def txt(el, tag):
    """Extract XML text and normalize whitespace to a single space."""
    v = el.findtext(tag, "")
    return re.sub(r"\s+", " ", v).strip() if v else ""


def name_txt(el, tag):
    """Semantic alias for name-like fields; keeps normalization consistent."""
    return txt(el, tag)


def export_daybook_to_path(
    start: str, end: str, out_path: str | None = None, progress_cb=None,
    company: str | None = None,
) -> Path:
    """Fetch the complete daybook from Tally and write a normalized XML file.

    Args:
        start: Start date in DD-MM-YYYY format.
        end: End date in DD-MM-YYYY format.
        out_path: Optional output file path. Defaults to daybook_DDMMYYYY_to_DDMMYYYY.xml.
        progress_cb: Optional callable ``(done_chunks, total_chunks, label)`` invoked
            after each monthly chunk is fetched, for UI progress reporting.
        company: Pin the request to this Tally company via ``<SVCURRENTCOMPANY>``. When
            None, runs against whatever company is active (legacy behaviour).

    Returns:
        Path to the written XML file.

    The fetch is split into monthly chunks to avoid Tally HTTP read-timeouts
    on large date ranges. Vouchers are deduplicated by GUID across chunks.
    The output XML groups all vouchers under a <TALLYDAYBOOK> root with
    normalized field names, ledger entries, inventory entries, and allocations.
    """
    start_s = start.strip()
    end_s = end.strip()
    start_dt = datetime.strptime(start_s, "%d-%m-%Y")
    end_dt = datetime.strptime(end_s, "%d-%m-%Y")
    if end_dt < start_dt:
        raise ValueError("END date must be on or after START date.")

    start_tally = to_tally_date(start_s)
    end_tally = to_tally_date(end_s)
    span_days = (end_dt - start_dt).days + 1
    print(f"\nFetching: {start_tally} to {end_tally} ({span_days} days) ...")

    # ── Fetch month-by-month to avoid Tally read-timeout on large ranges ──
    # Tally's raw <TYPE>Voucher</TYPE> collection is not strictly clipped to
    # SVFROMDATE/SVTODATE, so it can return vouchers outside the requested period
    # (e.g. post-dated cheques beyond the year-end). Guard on the voucher DATE —
    # still in raw YYYYMMDD form here — so out-of-period vouchers never leak through.
    start_key = start_dt.strftime("%Y%m%d")
    end_key = end_dt.strftime("%Y%m%d")
    dropped_out_of_range = 0
    all_vouchers = []
    seen_guid = set()  # Track GUIDs to deduplicate vouchers across overlapping chunks
    chunks = list(iter_month_chunks(start_dt, end_dt))
    for i, (cs, ce) in enumerate(chunks, 1):
        st = dt_to_tally(cs)
        et = dt_to_tally(ce)
        print(f"  Chunk {i}/{len(chunks)}: {st} … {et}")
        raw = post(envelope_daybook(st, et, company))
        root_xml = ET.fromstring(raw)
        for v in root_xml.findall(".//VOUCHER"):
            if not v.get("VCHTYPE", "").strip():
                continue
            # Drop vouchers dated outside the requested period. Only filter on a
            # well-formed YYYYMMDD date; a blank/short date is kept so downstream
            # handling is unchanged. Lexical compare is valid for zero-padded dates.
            d = txt(v, "DATE")
            if len(d) == 8 and (d < start_key or d > end_key):
                dropped_out_of_range += 1
                continue
            g = txt(v, "GUID")
            if g:
                if g in seen_guid:
                    continue
                seen_guid.add(g)
            all_vouchers.append(v)
        if progress_cb is not None:
            progress_cb(i, len(chunks), f"{st} … {et}")

    # ── Guard: drop vouchers belonging to a different company ──────────────
    # Defense-in-depth behind SVCURRENTCOMPANY pinning. If a Tally build ignores
    # the pin and returns vouchers from other loaded companies, keep only the
    # majority (home) company GUID and report the count — so foreign transactions
    # can never slip into the audit silently.
    dropped_foreign_company = 0
    prefixes = [p for p in (_company_guid(v) for v in all_vouchers) if p]
    if prefixes:
        home = Counter(prefixes).most_common(1)[0][0]
        kept = [v for v in all_vouchers if _company_guid(v) in ("", home)]
        dropped_foreign_company = len(all_vouchers) - len(kept)
        all_vouchers = kept
        if dropped_foreign_company:
            print(
                f"Dropped {dropped_foreign_company} voucher(s) from a different "
                f"company (kept company GUID {home})"
            )

    if dropped_out_of_range:
        print(
            f"Dropped {dropped_out_of_range} voucher(s) outside {start_s}..{end_s}"
        )
    print(f"Total vouchers fetched: {len(all_vouchers)}")

    # ── Voucher type summary ───────────────────────────────────────────────
    vtype_counts = {}
    for v in all_vouchers:
        vt = v.get("VCHTYPE", "Unknown")
        vtype_counts[vt] = vtype_counts.get(vt, 0) + 1
    print("\nBreakdown:")
    for vt, c in sorted(vtype_counts.items(), key=lambda x: -x[1]):
        print(f"  {vt:<30} {c:>6}")

    # ── Build output XML ───────────────────────────────────────────────────
    output_root = Element("TALLYDAYBOOK")
    output_root.set("FROMDATE",   start_tally)
    output_root.set("TODATE",     end_tally)
    output_root.set("TOTALCOUNT", str(len(all_vouchers)))

    for v in all_vouchers:
        vo = SubElement(output_root, "VOUCHER")
        vo.set("VCHTYPE",  v.get("VCHTYPE", ""))
        vo.set("OBJVIEW",  v.get("OBJVIEW", ""))
        vo.set("REMOTEID", v.get("REMOTEID", ""))
        vo.set("VCHKEY",   v.get("VCHKEY", ""))

        def add(tag, val):
            """Helper: append a child element with text to the current voucher node."""
            e = SubElement(vo, tag)
            e.text = val or ""

        # ── Header ────────────────────────────────────────────────────────
        add("DATE",            fmt_date(txt(v, "DATE")))
        add("EFFECTIVEDATE",   fmt_date(txt(v, "EFFECTIVEDATE")))
        add("VOUCHERTYPENAME", txt(v, "VOUCHERTYPENAME"))
        add("VOUCHERNUMBER",   txt(v, "VOUCHERNUMBER"))
        add("NUMBERSERIES",    txt(v, "VOUCHERNUMBERSERIES"))
        add("NUMBERINGSTYLE",  txt(v, "NUMBERINGSTYLE"))
        add("REFERENCENUMBER", txt(v, "REFERENCENUMBER"))
        add("REFERENCEDATE",   fmt_date(txt(v, "REFERENCEDATE")))
        add("GUID",            txt(v, "GUID"))
        add("MASTERID",        txt(v, "MASTERID"))
        add("ALTERID",         txt(v, "ALTERID"))
        add("VOUCHERKEY",      txt(v, "VOUCHERKEY"))
        add("PARTYLEDGERNAME", name_txt(v, "PARTYLEDGERNAME"))
        add(
            "PARTYNAME",
            name_txt(v, "PARTYMAILINGNAME") or name_txt(v, "BASICBUYERNAME"),
        )
        add("AMOUNT",          txt(v, "AMOUNT"))
        add("NARRATION",       txt(v, "NARRATION"))
        add("PERSISTEDVIEW",   txt(v, "PERSISTEDVIEW"))

        # ── GST & supply details (from postman fields) ────────────────────
        add("GSTREGISTRATIONTYPE",      txt(v, "GSTREGISTRATIONTYPE"))
        add("PLACEOFSUPPLY",            txt(v, "PLACEOFSUPPLY"))
        add("REFERENCE",                txt(v, "REFERENCE"))
        add("COSTCENTRENAME",           txt(v, "COSTCENTRENAME"))
        add("PARTYGSTIN",               txt(v, "PARTYGSTIN"))
        add("CONSIGNEEGSTIN",           txt(v, "CONSIGNEEGSTIN"))
        add("STATENAME",                txt(v, "STATENAME"))
        add("CONSIGNEESTATENAME",       txt(v, "CONSIGNEESTATENAME"))
        add("CMPGSTSTATE",              txt(v, "CMPGSTSTATE"))
        add("CMPGSTIN",                 txt(v, "CMPGSTIN"))
        add("GSTREGISTRATION",          txt(v, "GSTREGISTRATION"))
        add("CMPGSTREGISTRATIONTYPE",   txt(v, "CMPGSTREGISTRATIONTYPE"))
        add("BASICBUYERNAME",           name_txt(v, "BASICBUYERNAME"))
        add("BASICDATETIMEOFINVOICE",   txt(v, "BASICDATETIMEOFINVOICE"))

        # ── Status flags ──────────────────────────────────────────────────
        add("ISOPTIONAL",               txt(v, "ISOPTIONAL"))
        add("ISCANCELLED",              txt(v, "ISCANCELLED"))
        add("ISDELETED",                txt(v, "ISDELETED"))
        add("ISINVOICE",                txt(v, "ISINVOICE"))
        add("ISDEEMEDPOSITIVE",         txt(v, "ISDEEMEDPOSITIVE"))
        add("ASORIGINAL",               txt(v, "ASORIGINAL"))
        add("ASPAYSLIP",                txt(v, "ASPAYSLIP"))
        add("ISDELETEDVCHRETAINED",     txt(v, "ISDELETEDVCHRETAINED"))
        add("ISECOMMERCESUPPLY",        txt(v, "ISECOMMERCESUPPLY"))
        add("ISREVERSECHARGEAPPLICABLE", txt(v, "ISREVERSECHARGEAPPLICABLE"))
        add("VOUCHERRETAINKEY",         txt(v, "VOUCHERRETAINKEY"))

        # ── Ledger entries ────────────────────────────────────────────────
        ledger_section = SubElement(vo, "LEDGERENTRIES")

        for le in v.findall("ALLLEDGERENTRIES.LIST"):
            lname = name_txt(le, "LEDGERNAME")
            if not lname:
                continue
            entry = SubElement(ledger_section, "ENTRY")

            def ladd(tag, val):
                """Helper: append a child element with text to the current ledger entry."""
                e = SubElement(entry, tag)
                e.text = val or ""

            ladd("LEDGERNAME",               lname)
            ladd("AMOUNT",                   txt(le, "AMOUNT"))
            ladd("ISDEEMEDPOSITIVE",         txt(le, "ISDEEMEDPOSITIVE"))
            ladd("ISPARTYLEDGER",            txt(le, "ISPARTYLEDGER"))
            ladd("ISLASTDEEMEDPOSITIVE",     txt(le, "ISLASTDEEMEDPOSITIVE"))
            ladd("TDSPARTYNAME",             name_txt(le, "TDSPARTYNAME"))
            ladd("APPROPRIATEFOR",           txt(le, "APPROPRIATEFOR"))
            ladd("TAXCLASSIFICATIONNAME",    txt(le, "TAXCLASSIFICATIONNAME"))
            ladd("GSTCLASS",                 txt(le, "GSTCLASS"))
            ladd("GSTHSNNAME",               txt(le, "GSTHSNNAME"))
            ladd("GSTHSNSACCODE",            txt(le, "GSTHSNSACCODE"))
            ladd("GSTHSNDESCRIPTION",        txt(le, "GSTHSNDESCRIPTION"))
            ladd("GSTTAXRATE",               txt(le, "GSTTAXRATE"))
            ladd("GSTASSESSABLEVALUE",       txt(le, "GSTASSESSABLEVALUE"))
            ladd("IGSTLIABILITY",            txt(le, "IGSTLIABILITY"))
            ladd("CGSTLIABILITY",            txt(le, "CGSTLIABILITY"))
            ladd("SGSTLIABILITY",            txt(le, "SGSTLIABILITY"))
            ladd("GSTCESSLIABILITY",         txt(le, "GSTCESSLIABILITY"))
            ladd("GSTOVRDNTAXABILITY",       txt(le, "GSTOVRDNTAXABILITY"))
            ladd("GSTOVRDNSTOREDNATURE",     txt(le, "GSTOVRDNSTOREDNATURE"))
            ladd("GSTOVRDNINELIGIBLEITC",    txt(le, "GSTOVRDNINELIGIBLEITC"))
            ladd("GSTOVRDNISREVCHARGEAPPL",  txt(le, "GSTOVRDNISREVCHARGEAPPL"))
            ladd("GSTOVRDNASSESSABLEVALUE", txt(le, "GSTOVRDNASSESSABLEVALUE"))
            ladd("GSTOVRDNCLASSIFICATION",  txt(le, "GSTOVRDNCLASSIFICATION"))
            ladd("GSTOVRDNNATURE",          txt(le, "GSTOVRDNNATURE"))
            ladd("LEDGERFROMITEM",          txt(le, "LEDGERFROMITEM"))
            ladd("VATEXPAMOUNT",             txt(le, "VATEXPAMOUNT"))
            ladd("VATASSESSABLEVALUE",       txt(le, "VATASSESSABLEVALUE"))

            # GST rate details
            rates = SubElement(entry, "RATEDETAILS")
            for rd in le.findall("RATEDETAILS.LIST"):
                dh = txt(rd, "GSTRATEDUTYHEAD")
                if not dh:
                    continue
                r = SubElement(rates, "RATE")
                SubElement(r, "DUTYHEAD").text      = dh
                SubElement(r, "VALUATIONTYPE").text = txt(rd, "GSTRATEVALUATIONTYPE")
                SubElement(r, "RATE").text          = txt(rd, "GSTRATE")

            # Bill allocations
            bills = SubElement(entry, "BILLALLOCATIONS")
            for ba in le.findall("BILLALLOCATIONS.LIST"):
                bname = txt(ba, "NAME")
                if not bname:
                    continue
                b = SubElement(bills, "BILL")
                SubElement(b, "NAME").text             = bname
                SubElement(b, "BILLTYPE").text         = txt(ba, "BILLTYPE")
                SubElement(b, "AMOUNT").text           = txt(ba, "AMOUNT")
                SubElement(b, "BILLDATE").text         = fmt_date(txt(ba, "BILLDATE"))
                SubElement(b, "BILLCREATIONDATE").text = fmt_date(txt(ba, "BILLCREATIONDATE"))
                SubElement(b, "BILLID").text           = txt(ba, "BILLID")
                SubElement(b, "BILLCREDITPERIOD").text = txt(ba, "BILLCREDITPERIOD")

            # Bank allocations
            banks = SubElement(entry, "BANKALLOCATIONS")
            for bk in le.findall("BANKALLOCATIONS.LIST"):
                bkname = txt(bk, "NAME") or txt(bk, "TRANSACTIONNAME")
                if not bkname:
                    continue
                bke = SubElement(banks, "BANK")
                SubElement(bke, "NAME").text                  = bkname
                SubElement(bke, "AMOUNT").text                = txt(bk, "AMOUNT")
                SubElement(bke, "DATE").text                  = fmt_date(txt(bk, "DATE"))
                SubElement(bke, "INSTRUMENTDATE").text        = fmt_date(txt(bk, "INSTRUMENTDATE"))
                SubElement(bke, "PAYMENTMODE").text           = txt(bk, "PAYMENTMODE")
                SubElement(bke, "TRANSACTIONTYPE").text       = txt(bk, "TRANSACTIONTYPE")
                SubElement(bke, "TRANSFERMODE").text          = txt(bk, "TRANSFERMODE")
                SubElement(bke, "BANKNAME").text              = txt(bk, "BANKNAME")
                SubElement(bke, "ACCOUNTNUMBER").text         = txt(bk, "ACCOUNTNUMBER")
                SubElement(bke, "IFSCODE").text               = txt(bk, "IFSCODE")
                SubElement(bke, "UNIQUEREFERENCENUMBER").text = txt(bk, "UNIQUEREFERENCENUMBER")
                SubElement(bke, "PAYMENTFAVOURING").text      = name_txt(bk, "PAYMENTFAVOURING")
                SubElement(bke, "STATUS").text                = txt(bk, "STATUS")
                SubElement(bke, "BANKPARTYNAME").text         = name_txt(bk, "BANKPARTYNAME")
                SubElement(bke, "CHEQUECROSSCOMMENT").text    = txt(bk, "CHEQUECROSSCOMMENT")

            # TDS allocations
            tds_section = SubElement(entry, "TDSALLOCATIONS")
            for td in le.findall("TDSEXPENSEALLOCATIONS.LIST"):
                nop = txt(td, "NATUREOFPAYMENT")
                if not nop:
                    continue
                t = SubElement(tds_section, "TDS")
                SubElement(t, "NATUREOFPAYMENT").text    = nop
                SubElement(t, "TDSASSESSABLEVALUE").text = txt(td, "TDSASSESSABLEVALUE")
                SubElement(t, "ISTDSDEDUCTED").text      = txt(td, "ISTDSDEDUCTED")

        # ── Inventory entries ─────────────────────────────────────────────
        inv_section = SubElement(vo, "INVENTORYENTRIES")
        for ie in v.findall("ALLINVENTORYENTRIES.LIST"):
            iname = txt(ie, "STOCKITEMNAME") or txt(ie, "ITEMNAME")
            if not iname:
                continue
            inv = SubElement(inv_section, "ITEM")
            SubElement(inv, "STOCKITEMNAME").text = iname
            SubElement(inv, "AMOUNT").text        = txt(ie, "AMOUNT")
            SubElement(inv, "ACTUALQTY").text     = txt(ie, "ACTUALQTY")
            SubElement(inv, "BILLEDQTY").text     = txt(ie, "BILLEDQTY")
            SubElement(inv, "RATE").text          = txt(ie, "RATE")
            SubElement(inv, "DISCOUNT").text      = txt(ie, "DISCOUNT")
            SubElement(inv, "GODOWNNAME").text    = txt(ie, "GODOWNNAME")
            SubElement(inv, "BATCHNAME").text     = txt(ie, "BATCHNAME")
            SubElement(inv, "GSTHSNNAME").text    = txt(ie, "GSTHSNNAME")
            SubElement(inv, "GSTHSNSACCODE").text = txt(ie, "GSTHSNSACCODE")

    # ── Save ───────────────────────────────────────────────────────────────
    out_file = (
        Path(out_path)
        if out_path
        else Path(
            f"daybook_{datetime.strptime(start_s, '%d-%m-%Y').strftime('%d%m%Y')}_to_{datetime.strptime(end_s, '%d-%m-%Y').strftime('%d%m%Y')}.xml"
        )
    )
    indent(output_root, space="  ")
    ElementTree(output_root).write(str(out_file), encoding="unicode", xml_declaration=True)

    print(f"\nExport complete → {out_file}")
    print(f"Total vouchers  : {len(all_vouchers)}")
    print(f"\nBy voucher type:")
    for vt, c in sorted(vtype_counts.items(), key=lambda x: -x[1]):
        print(f"  {vt:<30} {c:>6}")
    return out_file


def main() -> None:
    p = argparse.ArgumentParser(
        description="Export daybook from Tally at http://localhost:9000.",
    )
    p.add_argument(
        "--start",
        dest="start_date",
        metavar="DD-MM-YYYY",
        help="Start date (use with --end; omit both for interactive prompts)",
    )
    p.add_argument(
        "--end",
        dest="end_date",
        metavar="DD-MM-YYYY",
        help="End date",
    )
    p.add_argument(
        "--out",
        help="Output XML file path (default: daybook_DDMMYYYY_to_DDMMYYYY.xml in cwd)",
    )
    p.add_argument(
        "--company",
        default=None,
        help="Pin the export to this Tally company (avoids mixing when several are open).",
    )
    args = p.parse_args()
    if args.start_date and args.end_date:
        export_daybook_to_path(
            args.start_date.strip(), args.end_date.strip(), args.out,
            company=args.company,
        )
        return
    if args.start_date or args.end_date:
        p.error("Provide both --start and --end, or neither for interactive mode.")
    print("=" * 55)
    print("   Tally DayBook Complete XML Exporter")
    print("=" * 55)
    start = input("Enter START date (DD-MM-YYYY): ").strip()
    end = input("Enter END date   (DD-MM-YYYY): ").strip()
    export_daybook_to_path(start, end, args.out, company=args.company)


if __name__ == "__main__":
    main()