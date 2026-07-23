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
    party_names: frozenset[str] = frozenset(),
) -> str:
    """Classify a single ledger from its master fields + reserved-group closures.

    ``fields`` is a flattened ledger-master dict (from
    ``to_json.extract_ledger_master_fields``): PARENT, ROOTPRIMARY, NATURE, TAXTYPE.
    ``party_names`` is the set of already-``_norm``-alised NAMEs of ledgers the
    audit treats as parties in their own right (they have their own voucher file);
    such a ledger is a real counter-party and must never fall through to a silent
    non-payee ``asset``.
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
    in_closure = parent in closures["tax"] or parent in closures["provision"]
    in_tax_family = (
        in_closure
        or by_group_name is not None
        or by_taxtype is not None
    )
    if in_tax_family:
        # ── NATURE veto ──────────────────────────────────────────────────────
        # A statutory/tax OFFSET is a balance-sheet (Liability/Asset) concept. A
        # ledger whose Tally NATURE is a P&L Expense/Income is base-eligible by
        # definition and can NEVER be an offset — so a fuzzy tax-family signal must
        # not demote it. This is the durable fix for the "provision" substring
        # matching the STANDARD EXPENSE group "Payment to and Provision for
        # Employees" (Staff Uniform, Staff Salary, Staff Welfare, Exgratia…): a
        # group NAME keyword or TAXTYPE can never override NATURE for a P&L line.
        # A reserved Duties&Taxes/Provisions CLOSURE membership that still
        # contradicts a P&L nature is a genuine conflict → REVIEW (CA decides),
        # never a silent statutory tag that would erase the TDS base.
        if nature == "expense":
            return REVIEW if in_closure else EXPENSE
        if nature == "income":
            return REVIEW if in_closure else INCOME
        # Liability/Asset (or blank) nature — the real tax/statutory family.
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
    # P&L first: a genuine payee is never a P&L line, and the audit list can carry
    # a stray expense/income ledger — it must NOT be turned into a payee.
    if nature == "expense":
        return EXPENSE
    if nature == "income":
        return INCOME

    # A ledger the audit analyses as a party in its own right (it has its own
    # voucher file → its NAME is in party_names) is a real counter-party — so when
    # the SAME ledger appears as a credit inside another party's voucher it is a
    # genuine second payee, never a silent asset. This rescues trade parties booked
    # under a custom asset/receivable group (e.g. "TRADE A R") with no PAN/GSTIN,
    # which the closures and tax/PAN rungs above cannot catch. Structural
    # (is-it-a-party), not name-based; placed AFTER the P&L check and BEFORE the
    # asset/review fall-through, so it only ever rescues a balance-sheet party and
    # can never touch a real prepaid/deposit or a P&L line. (bank/tax ledgers were
    # already resolved in Tier 1, so a provision or bank ledger that leaked into
    # the audit list stays statutory/bank, never a payee.)
    if name_norm and name_norm in party_names:
        return PARTY

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
    party_names: frozenset[str] | set[str] | None = None,
) -> dict[str, str]:
    """Deterministically classify every ledger in the master index.

    ``party_names`` (optional) is the set of raw ledger NAMEs the audit analyses
    as parties in their own right; normalised once here and used to keep a proven
    party from being classified as a silent non-payee asset. Omitting it leaves
    behavior byte-for-byte unchanged.

    Returns ``{ledger_name: class}``. No LLM, no network — Tier 1/2 only; the
    residual is ``REVIEW``. Callers that want the optional LLM tier run
    ``refine_review_with_llm`` over the review-class names afterwards.
    """
    closures = _build_closures(groups_xml)
    party_norm = frozenset(_norm(n) for n in (party_names or ()))
    return {
        name: classify_one(fields, closures, party_norm)
        for name, fields in ledger_index.items()
    }


# ── LLM tier: classify the deterministic REVIEW residual ─────────────────────
# The classes the LLM may assign. It must be CONFIDENT; anything else stays
# REVIEW and is escalated to the human "Classify ledgers" tab.
_LLM_CLASSES = frozenset({PARTY, TDS, TCS, GST, STATUTORY, BANK, EXPENSE, INCOME, ASSET})


def _llm_cache_key(name: str, group: str) -> str:
    # Group is part of the key: the same name can mean different things under
    # different groups across clients, and the cache may be shared.
    return f"{name.lower()}||{group.lower()}"


def _llm_system_prompt() -> str:
    return (
        "You are an expert in Indian accounting (Tally) and Indian Income-Tax TDS. "
        "For each ledger you are given its NAME, its Tally PARENT GROUP, and its "
        "nature. Decide WHAT KIND of account it is — pick exactly one class:\n\n"
        "  party     — a real counter-party who is PAID: vendor, contractor, "
        "professional, landlord, transporter, or an EMPLOYEE, or a debtor. TDS may apply.\n"
        "  tds       — a TDS tax ledger (tax deducted at source; payable or receivable).\n"
        "  tcs       — a TCS tax ledger (section 206C).\n"
        "  gst       — a GST tax ledger (CGST/SGST/IGST/UGST input, output, RCM, cash/credit ledger).\n"
        "  statutory — an impersonal statutory / adjustment POOL that is NOT a party and "
        "NOT a specific tax subtype: PF/ESI/PT payable, gratuity/bonus provision, "
        "round-off / excess-shortage, retention, 'Duties & Taxes' bucket, suspense.\n"
        "  bank      — a bank or cash ledger.\n"
        "  expense   — a Profit & Loss expense.\n"
        "  income    — a Profit & Loss income.\n"
        "  asset     — a balance-sheet asset that is neither a party nor a tax ledger: "
        "prepaid, advance, deposit, fixed asset, loan given.\n\n"
        "RULES\n"
        "- Judge NAME and GROUP together; the group is a strong hint. Real books have "
        "typos and abbreviations — judge by intent, never require exact spelling.\n"
        "- A person's name, or a business marker (Pvt Ltd, LLP, & Co, & Associates, "
        "Enterprises, Traders), is a PARTY even if grouped oddly.\n"
        "- SAFETY BIAS: if you are torn between 'party' and 'statutory', choose 'party' — "
        "wrongly calling a real payee 'statutory' hides a TDS liability. Wrongly calling a "
        "pool 'party' only adds one review row.\n"
        "- CONFIDENCE: set confident=false whenever you cannot decide the kind with high "
        "certainty — those go to a human. Give a confident answer when name+group is clear; "
        "only defer the genuinely ambiguous ones.\n\n"
        "Return via record_classifications: one entry per input in the SAME order, "
        "{name (verbatim, no [group] annotation), ledger_class (one of the 9, or 'unknown' "
        "when not confident), confident}."
    )


def _llm_tool_schema() -> dict[str, Any]:
    return {
        "name": "record_classifications",
        "description": "Record the ledger-kind classification for the batch. Call once "
                       "with one entry per input name, preserving order.",
        "input_schema": {
            "type": "object",
            "properties": {"decisions": {"type": "array", "items": {"type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Ledger name verbatim, "
                             "exactly as given before the [group: ...] annotation."},
                    "ledger_class": {"type": "string", "enum": sorted(_LLM_CLASSES) + ["unknown"]},
                    "confident": {"type": "boolean", "description": "True only if the kind "
                                  "is clear; false sends the ledger to a human."},
                },
                "required": ["name", "ledger_class", "confident"]}}},
            "required": ["decisions"],
        },
    }


def _default_llm_call(batch: list[tuple[str, str, str]], *, api_key, model) -> list[dict]:
    """One LLM call over (name, group, nature) triples → raw decision dicts.

    Reuses the same client/thinking/forced-tool pattern as the party blocklist.
    """
    from tds.apply_party_blocklist import _make_client
    from tds.apply_expense_blocklist import _thinking_config
    import os

    client = _make_client(api_key)
    user_msg = (
        f"Classify these {len(batch)} ledgers. Return one decision per ledger in the "
        f"same order via record_classifications.\n\n" + "\n".join(
            f"{i + 1}. {name} [group: {group or '(unknown)'}] [nature: {nature or '(unknown)'}]"
            for i, (name, group, nature) in enumerate(batch)
        )
    )
    run_id = os.environ.get("TDS_RUN_ID", "").strip()
    extra = {"extra_headers": {"X-Run-Id": run_id}} if run_id else {}
    message = client.messages.create(
        model=model, max_tokens=8000, thinking=_thinking_config(model, False),
        system=[{"type": "text", "text": _llm_system_prompt(),
                 "cache_control": {"type": "ephemeral"}}],
        tools=[_llm_tool_schema()],
        tool_choice={"type": "tool", "name": "record_classifications"},
        messages=[{"role": "user", "content": user_msg}], timeout=600.0, **extra,
    )
    block = next((b for b in message.content if getattr(b, "type", None) == "tool_use"), None)
    if block is None:
        raise RuntimeError(f"Model did not call the tool (stop={message.stop_reason!r})")
    return (block.input or {}).get("decisions") or []


def classify_review_with_llm(
    classes: dict[str, str],
    ledger_index: dict[str, dict[str, Any]],
    *,
    cache_path: Path | None = None,
    api_key: str | None = None,
    model: str = "claude-haiku-4-5",
    batch_size: int = 25,
    llm_call=None,
    progress: bool = True,
) -> tuple[dict[str, str], dict[str, Any]]:
    """LLM tier — resolve the deterministic REVIEW residual so only genuinely
    ambiguous ledgers reach the human 'Classify ledgers' tab.

    For each ledger the deterministic tiers left as REVIEW, ask the model (name +
    parent group + nature) for its kind. A CONFIDENT, valid verdict updates the
    class; anything else (not confident / 'unknown' / invalid / LLM error) stays
    REVIEW → escalated to a human. Per-ledger decisions are cached on disk
    (keyed name||group), so re-runs are free. Returns ``(classes, report)``.

    ``llm_call`` is injectable for tests: ``llm_call(batch) -> [{name,
    ledger_class, confident}, ...]``. When None, the real cached Anthropic client
    is used; if no API access is configured the residual is left untouched.
    """
    review_names = [n for n, c in classes.items() if c == REVIEW]
    report = {"review_in": len(review_names), "resolved": 0, "escalated": 0,
              "cache_hits": 0, "llm_error": None}
    if not review_names:
        return classes, report

    from tds.apply_expense_blocklist import load_cache, save_cache
    cache: dict[str, dict] = load_cache(cache_path) if cache_path else {}

    def _fields(n):
        f = ledger_index.get(n, {})
        return (n, (f.get("PARENT") or "").strip(), (f.get("NATURE") or "").strip())

    def _apply(n, verdict):
        cls = str(verdict.get("ledger_class", "")).strip().lower()
        if verdict.get("confident") and cls in _LLM_CLASSES:
            classes[n] = cls
            report["resolved"] += 1
        else:
            report["escalated"] += 1

    todo: list[tuple[str, str, str]] = []
    for n in review_names:
        _, group, nature = _fields(n)
        cached = cache.get(_llm_cache_key(n, group))
        if cached is not None:
            report["cache_hits"] += 1
            _apply(n, cached)
        else:
            todo.append((n, group, nature))

    if progress:
        print(f"[classify_ledgers] LLM tier — review: {len(review_names)} | "
              f"cache hits: {report['cache_hits']} | to classify: {len(todo)}",
              file=sys.stderr)

    if todo:
        call = llm_call or (lambda b: _default_llm_call(b, api_key=api_key, model=model))
        batches = [todo[i:i + batch_size] for i in range(0, len(todo), batch_size)]
        for bi, batch in enumerate(batches):
            try:
                decisions = call(batch)
            except Exception as exc:  # noqa: BLE001 — LLM failure must never crash export
                report["llm_error"] = str(exc)
                if progress:
                    print(f"[classify_ledgers] LLM batch {bi + 1}/{len(batches)} failed "
                          f"({exc}); leaving {len(batch)} ledger(s) for human review.",
                          file=sys.stderr)
                for n, _g, _nat in batch:
                    report["escalated"] += 1
                continue
            by_name = {}
            for d in decisions:
                if isinstance(d, dict):
                    nm = str(d.get("name", "")).split("[group:", 1)[0].strip()
                    if nm:
                        by_name[nm.lower()] = d
            for n, group, _nat in batch:
                v = by_name.get(n.lower(), {"ledger_class": "unknown", "confident": False})
                _apply(n, v)
                if cache_path is not None:
                    cache[_llm_cache_key(n, group)] = {
                        "ledger_class": str(v.get("ledger_class", "unknown")).strip().lower(),
                        "confident": bool(v.get("confident")),
                    }
            if cache_path is not None:
                save_cache(cache_path, cache)

    if progress:
        print(f"[classify_ledgers] LLM tier done — resolved: {report['resolved']} | "
              f"escalated to human: {report['escalated']}", file=sys.stderr)
    return classes, report


__all__ = [
    "PARTY", "TDS", "TCS", "GST", "STATUTORY", "BANK",
    "EXPENSE", "INCOME", "ASSET", "REVIEW",
    "STATUTORY_OFFSET_CLASSES",
    "classify_one", "classify_ledgers", "classify_review_with_llm",
]
