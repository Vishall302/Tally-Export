#!/usr/bin/env python3
"""
Shared ledger-set loading utilities used by the analysis pipeline.

Builds two sets of ledger display names from enriched Tally XML:
  - expense_or_fixed  : NATURE==Expense OR ROOTPRIMARY==Fixed Assets
  - liability_or_current: NATURE==Liability OR ROOTPRIMARY==Current Assets

Supports an optional pre-filtered expense override (TDS mode) via an explicit
path or auto-detection of a sidecar file next to the ledgers XML.
"""

from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


DEFAULT_FILTERED_EXPENSE_NAME = "expense_filtered.json"
"""
Conventional sidecar filename. When ``load_expense_and_liability_sets`` is
called with ``auto_detect=True`` and no explicit ``expense_override``, it looks
for this file next to the ledger master XML.
"""


def load_name_list(path: Path) -> set[str]:
    """
    Read a JSON array or one-name-per-line text file into a set of names.

    Used to load a pre-filtered expense set produced by
    ``apply_expense_blocklist.py``. Public so other scripts importing from this
    module can reuse it instead of duplicating the loader.
    """
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return set()
    if text.startswith("["):
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError(f"{path}: JSON root must be an array of strings")
        return {str(x).strip() for x in data if str(x).strip()}
    return {line.strip() for line in text.splitlines() if line.strip()}


def _resolve_expense_source(
    ledgers_xml: Path,
    expense_override: Path | None,
    auto_detect: bool,
) -> Path | None:
    """
    Decide which file (if any) to use for the expense_or_fixed override.

    Returns the file path to load from, or None if the expense set should be
    derived from XML. Emits clear log lines so every run is auditable.

    Resolution order:
      1. Explicit ``expense_override`` wins.
      2. ``auto_detect=False`` → no auto-detect, always XML.
      3. Sidecar at ``<ledgers_xml.parent>/expense_filtered.json`` if it exists.
      4. Otherwise, None (XML extraction).
    """
    if expense_override is not None:
        print(
            f"[core.ledger_sets] Using --filtered-expense: {expense_override}",
            file=sys.stderr,
        )
        return expense_override

    if not auto_detect:
        print(
            "[core.ledger_sets] --no-filter set: using raw XML expense classification.",
            file=sys.stderr,
        )
        return None

    sidecar = ledgers_xml.parent / DEFAULT_FILTERED_EXPENSE_NAME
    if not sidecar.is_file():
        print(
            "[core.ledger_sets] No filter sidecar found "
            f"({sidecar.name} not in {ledgers_xml.parent}); using raw XML "
            "expense classification.",
            file=sys.stderr,
        )
        return None

    # Sidecar found — staleness check vs the ledgers XML.
    try:
        sidecar_mtime = sidecar.stat().st_mtime
        ledgers_mtime = ledgers_xml.stat().st_mtime
    except OSError:
        sidecar_mtime = ledgers_mtime = 0.0

    if sidecar_mtime + 1 < ledgers_mtime:
        from datetime import datetime
        sidecar_when = datetime.fromtimestamp(sidecar_mtime).strftime("%Y-%m-%d %H:%M:%S")
        ledgers_when = datetime.fromtimestamp(ledgers_mtime).strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"[core.ledger_sets] WARNING: filter sidecar "
            f"{sidecar.name} ({sidecar_when}) is older than ledgers XML "
            f"({ledgers_when}). Filter may be stale; re-run "
            f"apply_expense_blocklist.py to refresh. Continuing with the "
            f"current sidecar.",
            file=sys.stderr,
        )
    else:
        print(
            f"[core.ledger_sets] Auto-detected filter sidecar: "
            f"{sidecar.name} (next to ledgers XML).",
            file=sys.stderr,
        )
    return sidecar


def load_expense_and_liability_sets(
    ledgers_xml: Path,
    expense_override: Path | None = None,
    auto_detect: bool = True,
) -> tuple[set[str], set[str]]:
    """
    Build two sets of ledger display names from the enriched ledgers XML.

    Mirrors:
      - ``list_expense_or_fixed_asset_ledgers.py``  → first set
      - ``list_liability_or_current_assets_ledgers.py`` → second set

    Uses incremental parsing and clears each ``<LEDGER>`` after processing to limit
    memory use on large files.

    Parameters
    ----------
    ledgers_xml : Path
        Path to the enriched TALLYLEDGERS XML.
    expense_override : Path or None
        Explicit pre-filtered expense names file (JSON array or one-name-per-line).
        When given, beats everything else (including auto-detect). Typically the
        output of ``apply_expense_blocklist.py``.
    auto_detect : bool
        If True (default), look for the conventional sidecar
        ``expense_filtered.json`` next to the ledgers XML and use it when found.
        Set to False to force raw-XML extraction (the ``--no-filter`` opt-out).
        Has no effect when ``expense_override`` is explicitly provided.

    The liability/current-assets set is always derived from XML.
    """
    source = _resolve_expense_source(ledgers_xml, expense_override, auto_detect)
    use_xml_for_expense = source is None

    expense_or_fixed: set[str] = set()
    liability_or_current: set[str] = set()

    for _event, elem in ET.iterparse(str(ledgers_xml), events=("end",)):
        if elem.tag != "LEDGER":
            continue
        name = (elem.get("NAME") or "").strip()
        if not name:
            elem.clear()
            continue
        nature = (elem.findtext("NATURE") or "").strip()
        rootprimary = (elem.findtext("ROOTPRIMARY") or "").strip()
        if use_xml_for_expense and (
            nature == "Expense" or rootprimary == "Fixed Assets"
        ):
            expense_or_fixed.add(name)
        if nature == "Liability" or rootprimary == "Current Assets":
            liability_or_current.add(name)
        elem.clear()

    if source is not None:
        expense_or_fixed = load_name_list(source)
        print(
            f"[core.ledger_sets] Loaded {len(expense_or_fixed)} "
            f"filtered expense names from {source.name}.",
            file=sys.stderr,
        )

    return expense_or_fixed, liability_or_current
