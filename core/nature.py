#!/usr/bin/env python3
"""
Shared, layered ledger/group nature classifier for Tally exports.

Historically nature (Income/Expense/Asset/Liability) was decided *only* by matching
a ledger's root group name against ~21 hardcoded Tally reserved primary-group names
(``PRIMARY_NATURE``). That silently mislabels any **custom-named primary group** an
accountant creates (e.g. a group literally named "Sales" under Primary instead of the
reserved "Sales Accounts") — the walk lands on "Primary" and the ledger never gets an
"Income" tag, so downstream TDS logic wrongly treats it as an expense.

This module replaces that with a layered, fail-safe classifier. Signals are tried in
order of how self-correcting they are, and anything unresolvable is quarantined as
``"Review"`` rather than silently defaulting into the expense/TDS path:

  1. Reserved-name fast-path .......... ``get_root_primary`` + ``PRIMARY_NATURE``
  2. Nature flags fallback ............ ``IsRevenue`` + ``IsDeemedPositive`` (root group,
                                        then the ledger's own)
  3. Closing-balance credit veto ...... a credit-balance ledger is *never* an expense
                                        (empirical debit/credit truth from real postings)
  4. Quarantine ....................... unresolved / conflicting -> ``"Review"``

The classifier accepts a ``groups`` map whose values may be a bare parent string
(legacy) or a dict carrying ``parent`` / ``isrevenue`` / ``isdeemedpositive`` keys.
"""

from __future__ import annotations


# Tally's fixed primary groups → (nature, financial statement). These names are set by
# Tally and identical across every company, so matching them is a legitimate fast-path.
# The gap this module closes is *custom* primary groups, which this table cannot know.
PRIMARY_NATURE: dict[str, tuple[str, str]] = {
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

_STATEMENT = {
    "Income":    "P&L",
    "Expense":   "P&L",
    "Asset":     "Balance Sheet",
    "Liability": "Balance Sheet",
}

# Sentinel nature for ledgers the layered classifier cannot resolve. Downstream code
# treats it as "not an expense, surface for review" — never a silent expense.
REVIEW = "Review"

_DEBIT_NATURES = {"Expense", "Asset"}


def _parent_of(groups: dict, name: str) -> str:
    """Parent group of *name*, tolerating both value shapes (str or dict)."""
    info = groups.get(name)
    if isinstance(info, dict):
        return (info.get("parent") or "").strip()
    return (info or "").strip()


def get_root_primary(name: str, groups: dict, depth: int = 0) -> str:
    """Walk the group parent chain up to the first Tally primary group name."""
    if depth > 20:
        return name
    if name in PRIMARY_NATURE:
        return name
    parent = _parent_of(groups, name)
    if not parent:
        return name
    return get_root_primary(parent, groups, depth + 1)


def _is_yes(value: str | None) -> bool:
    return (value or "").strip().lower() in {"yes", "true", "1"}


def _present(value: str | None) -> bool:
    return bool((value or "").strip())


def nature_from_flags(isrevenue: str | None, isdeemedpositive: str | None) -> str | None:
    """Derive nature from Tally's own flags. ``None`` when not determinable.

    IsRevenue      : Yes -> P&L (Income/Expense),  No -> Balance Sheet (Asset/Liability)
    IsDeemedPositive: Yes -> debit-natured (Expense/Asset), No -> credit-natured (Income/Liability)
    """
    if not _present(isrevenue) or not _present(isdeemedpositive):
        return None
    revenue = _is_yes(isrevenue)
    debit = _is_yes(isdeemedpositive)
    if revenue:
        return "Expense" if debit else "Income"
    return "Asset" if debit else "Liability"


def closing_balance_sign(closing_balance: str | None) -> str | None:
    """Return 'Cr', 'Dr', or None for a Tally CLOSINGBALANCE amount.

    Tally encodes a credit balance as a negative amount (and sometimes appends an
    explicit 'Cr'/'Dr'). A zero / blank / unparseable balance yields ``None`` (no signal).
    """
    if closing_balance is None:
        return None
    text = closing_balance.strip()
    if not text:
        return None
    lowered = text.lower().replace(" ", "")
    if lowered.endswith("cr"):
        return "Cr"
    if lowered.endswith("dr"):
        return "Dr"
    try:
        value = float(text.replace(",", ""))
    except ValueError:
        return None
    if value < 0:
        return "Cr"
    if value > 0:
        return "Dr"
    return None  # exactly zero -> no directional signal


def _nature_from_group_chain(parent: str, groups: dict) -> str | None:
    """Nature from the flags of the ledger's own group chain (parent upward).

    Used when the reserved-name walk is inconclusive (custom primary group). Tally
    inherits ``IsRevenue``/``IsDeemedPositive`` down the tree, so the ledger's own group
    carries the accountant's declared nature; walk upward until a group yields a nature.
    """
    name = parent
    for _ in range(21):
        info = groups.get(name)
        if isinstance(info, dict):
            resolved = nature_from_flags(
                info.get("isrevenue", ""), info.get("isdeemedpositive", "")
            )
            if resolved is not None:
                return resolved
            name = (info.get("parent") or "").strip()
        else:
            name = (info or "").strip()
        if not name or name in PRIMARY_NATURE:
            break
    return None


def classify_nature(
    parent: str,
    groups: dict,
    *,
    ledger_isrevenue: str | None = "",
    ledger_isdeemedpositive: str | None = "",
    closing_balance: str | None = "",
) -> tuple[str, str, str]:
    """Classify a ledger's nature. Returns ``(nature, financial_statement, root_primary)``.

    ``nature`` is one of Income / Expense / Asset / Liability / ``"Review"`` (quarantine).
    """
    root_primary = get_root_primary(parent, groups)

    # 1. Reserved-name fast-path (only a *real* primary counts, not Primary/Unknown).
    nature, _ = PRIMARY_NATURE.get(root_primary, (None, None))
    if nature == "Primary":
        nature = None

    # 2. Nature-flag fallback: the ledger's own group chain, then the ledger's own flags.
    group_flag_nature = None
    if nature is None:
        group_flag_nature = _nature_from_group_chain(parent, groups)
        nature = group_flag_nature
    if nature is None:
        nature = nature_from_flags(ledger_isrevenue, ledger_isdeemedpositive)

    # 3. Closing-balance credit veto (empirical, one-directional): a ledger carrying a
    #    credit balance cannot be a debit-natured account, so it is never an expense.
    if closing_balance_sign(closing_balance) == "Cr" and nature in _DEBIT_NATURES:
        revenue_signalled = _is_yes(ledger_isrevenue) or group_flag_nature in (
            "Income",
            "Expense",
        )
        # Prefer the confident reclassification (revenue -> Income); otherwise quarantine.
        nature = "Income" if revenue_signalled else None

    # 4. Quarantine anything still unresolved — never silently an expense.
    if nature is None:
        return (REVIEW, REVIEW, root_primary)

    return (nature, _STATEMENT.get(nature, "Unknown"), root_primary)
