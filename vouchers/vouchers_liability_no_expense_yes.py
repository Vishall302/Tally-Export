#!/usr/bin/env python3
"""
Scan a Tally daybook export and list **liability / current-asset** ledger names that
appear in “mixed” journal-style vouchers.

Background (Tally XML)
----------------------
- **Daybook** (`TALLYDAYBOOK`): contains `<VOUCHER>` elements. Each voucher has
  `<LEDGERENTRIES>` with one `<ENTRY>` per ledger line.
- **Ledgers** (`TALLYLEDGERS`): enriched export lists each `<LEDGER>` with
  `NATURE` and `ROOTPRIMARY` so we can classify ledgers the same way as the
  standalone list scripts.

**ISDEEMEDPOSITIVE** on an entry (not on the voucher root):
  - `Yes` → debit-like line in Tally’s accounting sense for that entry.
  - `No`  → credit-like line.

What this script finds 
----------------------
For each **voucher**, we test **all ledger lines in that voucher together**:

  1) There is at least one line where:
     - `LEDGERNAME` is classified as **Expense OR under Fixed Assets** (same rule as
       ``list_expense_or_fixed_asset_ledgers.py``), and
     - `ISDEEMEDPOSITIVE` is **Yes**.

  2) There is at least one line where:
     - `LEDGERNAME` is classified as **Liability OR under Current Assets** (same rule as
       ``list_liability_or_current_assets_ledgers.py``), and
     - `ISDEEMEDPOSITIVE` is **No**.

If **both** (1) and (2) hold in the **same** voucher, we record every distinct
liability/current-asset ledger name from the lines that satisfy (2) for that voucher.
Across the whole daybook, names are **deduplicated** (union), then **sorted** for output.

Ledger classification (must match the two list scripts)
--------------------------------------------------------
  - Expense / Fixed Assets set: ``NATURE == "Expense"`` OR ``ROOTPRIMARY == "Fixed Assets"``.
  - Liability / Current Assets set: ``NATURE == "Liability"`` OR ``ROOTPRIMARY == "Current Assets"``.

Performance
------------
Both XML files are read with **iterparse** and elements are **cleared** after use so
large exports (multi‑million lines) stay within reasonable memory.

Inputs / defaults
------------------
  - Ledgers: ``tally_ledgers_final.xml`` (same folder as this script unless ``--ledgers``).
  - Daybook: ``daybook_01042024_to_31032025.xml`` unless ``--daybook``.

TDS mode (auto-detect + opt-out)
--------------------------------
By default, every consumer of ``load_expense_and_liability_sets()`` auto-detects
a sidecar file named ``expense_filtered.json`` next to the ledger master XML
(typically the output of ``apply_expense_blocklist.py``). Resolution order:

  1. Explicit ``--filtered-expense FILE`` argument wins.
  2. ``--no-filter`` flag forces raw XML even if a sidecar exists.
  3. Otherwise, if ``<ledgers_xml.parent>/expense_filtered.json`` exists,
     it is loaded automatically.
  4. Otherwise, the expense set is built from XML (original behavior).

If the sidecar is older than the ledger master XML, a loud warning is printed
but the run continues — frontend orchestration shouldn't break, and the warning
is the audit trail. To re-freshen the filter, re-run ``apply_expense_blocklist.py``.

The override flows automatically into every importer (``final_list.py``,
``split_daybook_by_final_list.py``, ``tds_expense_wrapper.py``) — they don't
need to know anything about the sidecar; this function handles it.
The liability/current-assets set is always computed from XML.

Output
------
  - Default: write one ledger name per line (UTF‑8), sorted, to ``test.txt``
    next to this script (override with ``-o`` / ``--output``).
  - ``--json``: UTF‑8 JSON array of strings, sorted, same output file rules.

Usage
-----
  python vouchers_liability_no_expense_yes.py
  python vouchers_liability_no_expense_yes.py --daybook path.xml --ledgers path.xml
  python vouchers_liability_no_expense_yes.py --json
  python vouchers_liability_no_expense_yes.py -o /path/to/out.txt
  python vouchers_liability_no_expense_yes.py --filtered-expense expense_filtered.json
"""

from __future__ import annotations

import argparse
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
            f"[vouchers_liability_no_expense_yes] Using --filtered-expense: "
            f"{expense_override}",
            file=sys.stderr,
        )
        return expense_override

    if not auto_detect:
        print(
            "[vouchers_liability_no_expense_yes] --no-filter set: using raw XML "
            "expense classification.",
            file=sys.stderr,
        )
        return None

    sidecar = ledgers_xml.parent / DEFAULT_FILTERED_EXPENSE_NAME
    if not sidecar.is_file():
        print(
            "[vouchers_liability_no_expense_yes] No filter sidecar found "
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
        # Sidecar is older than ledgers — possible stale filter.
        from datetime import datetime
        sidecar_when = datetime.fromtimestamp(sidecar_mtime).strftime("%Y-%m-%d %H:%M:%S")
        ledgers_when = datetime.fromtimestamp(ledgers_mtime).strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"[vouchers_liability_no_expense_yes] WARNING: filter sidecar "
            f"{sidecar.name} ({sidecar_when}) is older than ledgers XML "
            f"({ledgers_when}). Filter may be stale; re-run "
            f"apply_expense_blocklist.py to refresh. Continuing with the "
            f"current sidecar.",
            file=sys.stderr,
        )
    else:
        print(
            f"[vouchers_liability_no_expense_yes] Auto-detected filter sidecar: "
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
            f"[vouchers_liability_no_expense_yes] Loaded {len(expense_or_fixed)} "
            f"filtered expense names from {source.name}.",
            file=sys.stderr,
        )

    return expense_or_fixed, liability_or_current


def collect_matching_liability_names(
    daybook_xml: Path,
    expense_or_fixed: set[str],
    liability_or_current: set[str],
) -> set[str]:
    """
    Walk every ``<VOUCHER>`` in the daybook; for vouchers that match the
    expense‑Yes + liability‑No pattern, collect all liability/current‑asset
    ledger names that appeared as ``ISDEEMEDPOSITIVE`` No in that voucher.

    Returns a set (unique names across all matching vouchers).
    """
    out: set[str] = set()
    for _event, voucher in ET.iterparse(str(daybook_xml), events=("end",)):
        if voucher.tag != "VOUCHER":
            continue

        expense_yes = False
        liability_no_names: list[str] = []

        ledge = voucher.find("LEDGERENTRIES")
        if ledge is not None:
            for entry in ledge.findall("ENTRY"):
                lname = (entry.findtext("LEDGERNAME") or "").strip()
                deemed = (entry.findtext("ISDEEMEDPOSITIVE") or "").strip()
                if lname in expense_or_fixed and deemed == "Yes":
                    expense_yes = True
                if lname in liability_or_current and deemed == "No":
                    liability_no_names.append(lname)

        if expense_yes and liability_no_names:
            out.update(liability_no_names)

        voucher.clear()

    return out


def main() -> None:
    base = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--ledgers",
        type=Path,
        default=base / "tally_ledgers_final.xml",
        help="Enriched TALLYLEDGERS XML (default: tally_ledgers_final.xml next to script)",
    )
    p.add_argument(
        "--daybook",
        type=Path,
        default=base / "daybook_01042024_to_31032025.xml",
        help="TALLYDAYBOOK XML (default: daybook next to script)",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Write a sorted JSON array instead of one name per line",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=base / "test.txt",
        help="Write the result list to this file (default: test.txt next to this script)",
    )
    p.add_argument(
        "--filtered-expense",
        type=Path,
        default=None,
        help="Optional: pre-filtered expense ledger names "
             "(JSON array or one-name-per-line text). When given, this set replaces "
             "the XML-derived expense_or_fixed set (typically the output of "
             "apply_expense_blocklist.py for TDS analysis). The liability/current-assets "
             "set is still computed from XML. Beats auto-detect.",
    )
    p.add_argument(
        "--no-filter",
        action="store_true",
        help="Force raw-XML expense classification even if a sidecar "
             f"({DEFAULT_FILTERED_EXPENSE_NAME}) exists next to the ledgers XML. "
             "Use this for non-TDS runs when you don't want the auto-detected "
             "filter to silently take effect. Ignored when --filtered-expense is also passed.",
    )
    args = p.parse_args()

    if not args.ledgers.is_file():
        print(f"Ledgers file not found: {args.ledgers}", file=sys.stderr)
        sys.exit(1)
    if not args.daybook.is_file():
        print(f"Daybook file not found: {args.daybook}", file=sys.stderr)
        sys.exit(1)
    if args.filtered_expense is not None and not args.filtered_expense.is_file():
        print(f"Filtered expense file not found: {args.filtered_expense}", file=sys.stderr)
        sys.exit(1)

    expense_or_fixed, liability_or_current = load_expense_and_liability_sets(
        args.ledgers,
        expense_override=args.filtered_expense,
        auto_detect=not args.no_filter,
    )
    names = collect_matching_liability_names(
        args.daybook, expense_or_fixed, liability_or_current
    )
    sorted_names = sorted(names)

    with args.output.open("w", encoding="utf-8", newline="\n") as f:
        if args.json:
            f.write(json.dumps(sorted_names, ensure_ascii=False, indent=2))
            f.write("\n")
        else:
            for n in sorted_names:
                f.write(n + "\n")


if __name__ == "__main__":
    main()    
