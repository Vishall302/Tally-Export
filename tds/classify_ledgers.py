#!/usr/bin/env python3
"""
Authoritative per-ledger CLASS for the TDS analyzer — decided once, at the
data-processing stage, from Tally's own GROUP structure rather than from
ledger-name string heuristics downstream.

Why this exists
---------------
The analyzer historically decided "is this credit a statutory account (TDS/GST/…)
or a second payee?" from the ledger NAME (``is_tds_ledger`` / ``is_statutory_credit``
in ``tds_analyzer/helpers.py``). Names are unbounded — typos ("TDS - Profeesional
Fee"), custom spellings, vendors that merely contain a tax word ("TDS Engineering")
— so every new client reopens the same bug. The durable signal is the ledger's
PARENT GROUP, which is clean even when the ledger name is a mess: a real TDS ledger
sits under "TDS Payable"/"Duties & Taxes"; a vendor sits under "Sundry Creditors".

This module assigns each ledger one CLASS the analyzer can trust by membership:

    party · tds · tcs · gst · statutory · bank · expense · income · asset · review

The ladder (first confident answer wins), mirroring ``core.nature``'s fail-safe
philosophy:

  1. GROUP (deterministic) — the ledger's PARENT resolved against the reserved
     Tally group closures (Duties & Taxes / Bank / Sundry Creditors & Debtors /
     Provisions) and, for custom group names, group-name keywords ("TDS Payable"
     → tds). Tally's per-ledger ``TAXTYPE`` is used when populated.
  2. NATURE — the already-computed Income/Expense/Asset nature (from
     ``core.nature.classify_nature``, emitted as ``NATURE`` on the master).
  3. LLM (optional) — only the still-ambiguous residual (a Liability/Asset under a
     generic group with no signal) is sent to the existing cached LLM classifier.
  4. REVIEW — anything still unresolved is quarantined, never silently guessed.

Conservative bias: a ledger is only tagged ``tds`` / demoted from ``party`` when a
confident signal says so. An ambiguous one becomes ``review`` (a visible CA
question), never a silent misclassification that could fabricate a TDS base.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.groups import parent_names_from_roots  # noqa: E402

# ── The class vocabulary ────────────────────────────────────────────────────
PARTY = "party"          # a real payee (vendor / contractor / professional / employee / debtor)
TDS = "tds"              # TDS tax ledger (withheld tax) — counts as "TDS deducted"
TCS = "tcs"              # TCS tax ledger (seller side)
GST = "gst"              # GST tax ledger
STATUTORY = "statutory"  # other Duties&Taxes / provisions / PF-ESI / round-off — non-payee offset
BANK = "bank"            # bank / cash
EXPENSE = "expense"      # P&L expense line
INCOME = "income"        # P&L income line
ASSET = "asset"          # balance-sheet asset that is neither a party nor a tax ledger
REVIEW = "review"        # unclassifiable — surfaced to the CA, never silently guessed

# Classes the analyzer treats as a non-payee statutory OFFSET (added back to the
# owner's gross credit and never counted as a second payee). ASSET/EXPENSE/INCOME
# are deliberately excluded — they are neither a payee nor a same-voucher offset.
STATUTORY_OFFSET_CLASSES = frozenset({TDS, TCS, GST, STATUTORY, BANK})

# Reserved Tally root groups whose subtree defines each class. Case-insensitive,
# subtree-expanded by core.groups (so custom sub-group names are still captured).
_PARTY_ROOTS = ["Sundry Creditors", "Sundry Debtors"]
_BANK_ROOTS = ["Bank Accounts", "Cash-in-Hand"]
_TAX_ROOTS = ["Duties & Taxes"]
_PROVISION_ROOTS = ["Provisions"]

# Group-NAME keyword signals for CUSTOM groups that sit outside the reserved
# closures (e.g. "TDS Payable"/"TDS Receivable" booked under Current Liabilities/
# Assets, not under "Duties & Taxes"). Matched as whole words on the parent-group
# name — group names are controlled by the accountant and far more stable than
# ledger names, and a group named "TDS Payable" holds tax ledgers, not vendors.
_TCS_GROUP_RE = re.compile(r"\btcs\b")
_TDS_GROUP_RE = re.compile(r"\btds\b|tax deducted")
_GST_GROUP_RE = re.compile(r"\b(gst|cgst|sgst|igst|ugst)\b")
# Statutory payroll / duty group names (whole-word for short tokens).
_STATUTORY_GROUP_RE = re.compile(
    r"duties\s*&?\s*taxes|duties and taxes|provident|gratuity|superannuation"
    r"|\bpf\b|\besi\b|\besic\b|\blwf\b|professional tax|profession tax"
    r"|labour welfare|labor welfare|provision"
)


def _norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _class_from_group_name(parent_norm: str) -> str | None:
    """Refine a tax/statutory class from a custom PARENT group NAME, or None."""
    if _GST_GROUP_RE.search(parent_norm):
        return GST
    if _TCS_GROUP_RE.search(parent_norm):
        return TCS
    if _TDS_GROUP_RE.search(parent_norm):
        return TDS
    if _STATUTORY_GROUP_RE.search(parent_norm):
        return STATUTORY
    return None


def _tax_subtype(*texts: str) -> str | None:
    """The GST/TCS/TDS subtype implied by any of *texts* (group and/or ledger
    name), or None. Used ONLY once a ledger is already known to be in the
    tax/statutory family (by group closure, group-name keyword, or TAXTYPE), so
    reading the ledger's own name here is safe from vendor collisions — a vendor
    ("TDS Engineering") is never in that family. GST is checked first so a
    "GST-TDS" pool is treated as GST, not vendor TDS."""
    for t in texts:
        if t and _GST_GROUP_RE.search(t):
            return GST
    for t in texts:
        if t and _TCS_GROUP_RE.search(t):
            return TCS
    for t in texts:
        if t and _TDS_GROUP_RE.search(t):
            return TDS
    return None


def _class_from_taxtype(taxtype_norm: str) -> str | None:
    """Tally's own per-ledger 'Type of Duty/Tax' when populated (often blank)."""
    if taxtype_norm == "gst":
        return GST
    if taxtype_norm == "tds":
        return TDS
    if taxtype_norm == "tcs":
        return TCS
    return None


def _build_closures(groups_xml: Path | None) -> dict[str, set[str]]:
    """Reserved-group subtree closures (parent-name sets) for each class.

    Returns empty sets when no groups XML is available (folder/upload path) — the
    classifier then leans on group-name keywords + NATURE + review.
    """
    if groups_xml is None or not Path(groups_xml).is_file():
        return {"party": set(), "bank": set(), "tax": set(), "provision": set()}
    gx = str(groups_xml)
    party, _ = parent_names_from_roots(gx, _PARTY_ROOTS)
    bank, _ = parent_names_from_roots(gx, _BANK_ROOTS)
    tax, _ = parent_names_from_roots(gx, _TAX_ROOTS)
    provision, _ = parent_names_from_roots(gx, _PROVISION_ROOTS)
    return {"party": party, "bank": bank, "tax": tax, "provision": provision}


def classify_one(
    fields: dict[str, Any],
    closures: dict[str, set[str]],
) -> str:
    """Classify a single ledger from its master fields + reserved-group closures.

    ``fields`` is a flattened ledger-master dict (from
    ``to_json.extract_ledger_master_fields``): PARENT, ROOTPRIMARY, NATURE, TAXTYPE.
    Returns one of the class constants; REVIEW when no confident signal exists.
    """
    parent = (fields.get("PARENT") or "").strip()
    parent_norm = _norm(parent)
    name_norm = _norm(fields.get("NAME"))
    nature = (fields.get("NATURE") or "").strip().lower()
    taxtype_norm = _norm(fields.get("TAXTYPE"))

    # ── Tier 1: GROUP ────────────────────────────────────────────────────────
    # Bank/cash first — unambiguous and never a tax ledger.
    if parent in closures["bank"]:
        return BANK

    # Is this ledger in the tax / statutory FAMILY? Any of: the Duties & Taxes or
    # Provisions closures, a tax/statutory group-name keyword, or Tally's TAXTYPE.
    by_taxtype = _class_from_taxtype(taxtype_norm)
    by_group_name = _class_from_group_name(parent_norm)
    in_tax_family = (
        parent in closures["tax"]
        or parent in closures["provision"]
        or by_group_name is not None
        or by_taxtype is not None
    )
    if in_tax_family:
        # Refine the subtype so a TDS ledger is recognised as 'tds' (→ counted as
        # TDS deducted) even under the generic "Duties & Taxes"/"Provisions" group.
        # Group/ledger NAME keyword FIRST — it is the reliable signal. Tally's
        # TAXTYPE is only a fallback: in real books it is usually blank and is
        # sometimes plain wrong (e.g. a "TDS Payable" ledger flagged TAXTYPE=GST),
        # so it must never override an explicit TDS/GST name. Safe from vendor
        # collisions: tax-family membership is already established.
        return (
            _tax_subtype(parent_norm, name_norm)
            or by_taxtype
            or STATUTORY
        )

    if parent in closures["party"]:
        return PARTY

    # Tax identity is a strong party signal: a ledger carrying its own PAN or
    # GSTIN is a real counter-party (vendor/professional/debtor), never a
    # statutory pool. Tax ledgers were already resolved above, so reaching here
    # with a PAN/GSTIN means a real party booked under a custom group that the
    # closures/keywords missed (e.g. a vendor "Payable" group). This rescues the
    # bulk of otherwise-REVIEW payables without guessing from the ledger name.
    pan = (fields.get("PAN") or fields.get("INCOMETAXNUMBER") or "").strip()
    gstin = (fields.get("GSTIN") or fields.get("PARTYGSTIN") or "").strip()
    if pan or gstin:
        return PARTY

    # ── Tier 2: NATURE (already resolved by core.nature at export time) ───────
    if nature == "expense":
        return EXPENSE
    if nature == "income":
        return INCOME
    if nature == "review":
        return REVIEW
    if nature == "asset":
        # A non-party, non-tax asset (prepaid/advance/deposit). Not a payee and
        # not a statutory offset — a neutral class the guard ignores.
        return ASSET

    # nature == "liability" (or blank) under a generic group with no tax/party
    # signal is genuinely ambiguous — a payable that could be statutory or a
    # party. Quarantine for the CA (or the optional LLM tier) rather than guess.
    return REVIEW


def classify_ledgers(
    ledger_index: dict[str, dict[str, Any]],
    groups_xml: Path | None = None,
) -> dict[str, str]:
    """Deterministically classify every ledger in the master index.

    Returns ``{ledger_name: class}``. No LLM, no network — Tier 1/2 only; the
    residual is ``REVIEW``. Callers that want the optional LLM tier run
    ``refine_review_with_llm`` over the review-class names afterwards.
    """
    closures = _build_closures(groups_xml)
    return {
        name: classify_one(fields, closures)
        for name, fields in ledger_index.items()
    }


def refine_review_with_llm(
    classes: dict[str, str],
    ledger_index: dict[str, dict[str, Any]],
    *,
    cache_path: Path | None = None,
    api_key: str | None = None,
    fail_on_llm_error: bool = False,
) -> dict[str, str]:
    """Optionally re-classify only the REVIEW-class residual with the cached LLM.

    Reuses the party-blocklist LLM plumbing (name + parent group, on-disk cache).
    Best-effort: on any LLM failure the residual stays REVIEW (safe). Returns the
    updated class map. This is a thin, opt-in tier — the deterministic Tier 1/2
    already resolves the overwhelming majority in real data.
    """
    review_names = [n for n, c in classes.items() if c == REVIEW]
    if not review_names:
        return classes
    try:
        from tds.apply_party_blocklist import (  # local import: optional dependency
            _make_client, _cache_key,
        )
        from tds.apply_expense_blocklist import load_cache, save_cache
    except Exception:  # noqa: BLE001
        return classes

    # The LLM here only decides party-vs-non-party; a "blocklisted" (non-party)
    # verdict leaves the ledger as STATUTORY, "kept" (real payee) promotes to PARTY.
    # Detailed tax-subtype refinement is left to the deterministic tiers. This keeps
    # the LLM cheap and its failure mode safe. Deliberately minimal; extend later.
    # (Implementation intentionally reuses the party-blocklist prompt/cache; wiring
    #  it in the pipeline is gated by API availability.)
    return classes  # placeholder wiring point — deterministic tiers are the default


__all__ = [
    "PARTY", "TDS", "TCS", "GST", "STATUTORY", "BANK",
    "EXPENSE", "INCOME", "ASSET", "REVIEW",
    "STATUTORY_OFFSET_CLASSES",
    "classify_one", "classify_ledgers", "refine_review_with_llm",
]
