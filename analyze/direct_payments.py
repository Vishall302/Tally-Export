#!/usr/bin/env python3
"""Direct payments — expenses paid straight to bank/cash with NO party ledger.

An accountant sometimes debits the expense and credits the bank in one voucher
(``Dr Professional Fees / Cr Bank``) without routing it through a party ledger.
The vendor, if named at all, appears only in the narration. Such a voucher
belongs to no party, so it is invisible to the party-centric TDS audit.

This module extracts those vouchers as a **standalone, read-only dataset** for
review. It is deliberately independent of the TDS pipeline:

* it never produces a ``VoucherAnalysis`` and never enters the results store, so
  it cannot reach the TDS dataframe, the dashboard, the exports, the PDF report
  or the chat — no party view has to filter it out, because it is never there;
* it computes no section, rate, threshold or status — this is a list of entries,
  not a compliance judgement;
* it derives its own ledger sets, so it runs even when TDS selection fails.

Qualification (all three must hold in one voucher — the same rule the party scan
uses in reverse, see ``detect_cross_vouchers``):

1. an **expense / fixed-asset** ledger on the **Dr** side (``ISDEEMEDPOSITIVE``
   Yes) — the expense actually booked;
2. a **bank or cash** ledger on the **Cr** side — a genuine outward payment,
   which distinguishes this from a reclassification journal;
3. **no genuine party ledger anywhere** in the voucher — if a party is present,
   the normal TDS path already covers it.

Amounts are the **expense** amount, not the bank outflow: for
``Dr Fees 100000 / Cr TDS 10000 / Cr Bank 90000`` this reports 100000, the
expense booked, which is the right basis for review.
"""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.groups import (  # noqa: E402
    DEFAULT_ROOT_GROUPS,
    ledgers_with_parent_in,
    parent_names_from_roots,
)
from core.ledger_sets import load_expense_and_liability_sets  # noqa: E402

# Bank/cash roots — the settlement side of a direct payment. A subset of
# DEFAULT_ROOT_GROUPS (no Duties/Branch): only these are a genuine outward payment.
SETTLEMENT_ROOT_GROUPS = ("Cash-in-Hand", "Bank Accounts")


def build_ledger_sets(
    ledgers_xml: Path, groups_xml: Path
) -> tuple[set[str], set[str], set[str]]:
    """``(expense_or_fixed, genuine_party_names, settlement_names)``.

    ``genuine_party_names`` is the liability/current-asset set with the
    group-excluded ledgers removed. That subtraction is essential, not cosmetic:
    many exports classify bank accounts under Current Assets, so without it a
    bank line reads as a "party", every direct payment looks party-backed, and
    the extractor silently reports nothing.

    ``auto_detect=True`` makes the expense set pick up the LLM-cleaned
    ``expense_filtered.json`` sidecar when the TDS selection has written one, so
    blocklisted junk ledgers (rounding, contra) don't pollute the list. Without
    the sidecar it falls back to the raw XML classification — noisier, but never
    wrong.
    """
    expense_or_fixed, liability_or_current = load_expense_and_liability_sets(
        ledgers_xml, auto_detect=True
    )
    excluded: set[str] = set()
    settlement_names: set[str] = set()
    if groups_xml and Path(groups_xml).is_file():
        parent_names, _missing = parent_names_from_roots(
            str(groups_xml), list(DEFAULT_ROOT_GROUPS)
        )
        excluded = set(ledgers_with_parent_in(str(ledgers_xml), parent_names))
        settlement_parents, _ = parent_names_from_roots(
            str(groups_xml), list(SETTLEMENT_ROOT_GROUPS)
        )
        settlement_names = set(ledgers_with_parent_in(str(ledgers_xml), settlement_parents))
    return expense_or_fixed, (liability_or_current - excluded), settlement_names


def _amount(entry: ET.Element) -> float:
    try:
        return abs(float((entry.findtext("AMOUNT") or "0").strip() or 0))
    except ValueError:
        return 0.0


def extract_direct_payments(
    daybook_xml: Path,
    expense_or_fixed: set[str],
    genuine_party_names: set[str],
    settlement_names: set[str],
) -> list[dict]:
    """Walk the daybook once and return one row per qualifying **expense line**.

    A voucher touching two expense ledgers yields two rows — one under each
    ledger, each with its own amount. That is the natural shape for a per-ledger
    listing and keeps every ledger's total correct.
    """
    rows: list[dict] = []
    for _event, voucher in ET.iterparse(str(daybook_xml), events=("end",)):
        if voucher.tag != "VOUCHER":
            continue

        expense_dr_lines: list[tuple[str, float]] = []
        has_settlement_cr = False
        has_genuine_party = False

        ledge = voucher.find("LEDGERENTRIES")
        if ledge is not None:
            for entry in ledge.findall("ENTRY"):
                lname = (entry.findtext("LEDGERNAME") or "").strip()
                deemed = (entry.findtext("ISDEEMEDPOSITIVE") or "").strip()
                if lname in expense_or_fixed and deemed == "Yes":
                    expense_dr_lines.append((lname, _amount(entry)))
                if lname in settlement_names and deemed == "No":
                    has_settlement_cr = True
                if lname in genuine_party_names:
                    has_genuine_party = True

        if expense_dr_lines and has_settlement_cr and not has_genuine_party:
            date = (voucher.findtext("DATE") or "").strip()
            vch_no = (voucher.findtext("VOUCHERNUMBER") or "").strip()
            narration = (voucher.findtext("NARRATION") or "").strip()
            vch_type = (voucher.findtext("VOUCHERTYPENAME") or "").strip()
            for lname, amount in expense_dr_lines:
                rows.append({
                    "expense_ledger": lname,
                    "date": date,
                    "voucher_number": vch_no,
                    "voucher_type": vch_type,
                    "narration": narration,
                    "amount": amount,
                })

        voucher.clear()

    return rows


def extract_from_export(
    daybook_xml: Path, ledgers_xml: Path, groups_xml: Path
) -> list[dict]:
    """Convenience entry point: derive the ledger sets, then extract. This is the
    whole public surface the pipeline needs."""
    expense_or_fixed, genuine_party_names, settlement_names = build_ledger_sets(
        ledgers_xml, groups_xml
    )
    if not settlement_names:
        # No bank/cash ledgers resolved (missing/unreadable groups export) — every
        # voucher would fail rule 2. Report nothing rather than guess.
        return []
    return extract_direct_payments(
        daybook_xml, expense_or_fixed, genuine_party_names, settlement_names
    )
