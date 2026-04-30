#!/usr/bin/env python3
"""
What this script does
---------------------
Reads Tally-export XML and works with one or more account-group roots (for example
"Duties & Taxes", "Cash-in-Hand", "Bank Accounts", "Branch / Divisions") and everything
nested under them in the group tree. You can either list those group names, or list every
ledger that sits directly under one of those groups (matched by the ledger's PARENT).

How it works (short)
--------------------
1. From the groups file, it finds all group names under selected root groups (by default:
   "Duties & Taxes", "Cash-in-Hand", "Bank Accounts", "Branch / Divisions"), including
   children, grandchildren, etc. (following PARENT links downward).
2. In ledger mode, it scans the ledgers file and prints each ledger's NAME when its
   <PARENT> text equals one of those group names.

Modes
-----
  Default: print ledger names (one per line) tied to selected root groups and subgroups.
  --groups-only (-G): print only the group names in that subtree (sorted).
  -v with -G: print BFS levels on stderr for debugging.

Inputs / defaults
-------------------
  Groups: tally_groups_final.xml   |  Ledgers: tally_ledgers_final.xml
  You can override paths with --groups-xml and --ledgers-xml.

Optional: give a text file (one group name per line) or pipe names on stdin to use as the
parent set instead of the auto closure from selected roots.

XML shape: groups use <GROUP><NAME> / <PARENT>; ledgers use <LEDGER NAME="..."><PARENT>.
"""

import argparse
import re
import sys
import xml.etree.ElementTree as ET


# Default Tally roots this script is anchored to (must match spelling in XML).
DEFAULT_ROOT_GROUPS = (
    "Duties & Taxes",
    "Cash-in-Hand",
    "Bank Accounts",
    "Branch / Divisions",
)


def normalize_group_name(name: str) -> str:
    """Case-insensitive matching helper for root-name lookup."""
    s = re.sub(r"\s*([/&])\s*", r" \1 ", name)
    return " ".join(s.split()).casefold()


def load_parent_name_pairs(xml_path: str) -> list[tuple[str, str]]:
    """Each Tally group row as (child name, parent name)."""
    tree = ET.parse(xml_path)
    root = tree.getroot()
    pairs: list[tuple[str, str]] = []
    for group in root.findall("GROUP"):
        name_el = group.find("NAME")
        parent_el = group.find("PARENT")
        if name_el is None or name_el.text is None:
            continue
        name = name_el.text.strip()
        parent = (parent_el.text or "").strip()
        pairs.append((name, parent))
    return pairs


def collect_descendants(pairs: list[tuple[str, str]], root: str) -> tuple[set[str], list[list[str]]]:
    """Breadth-first expansion: all group names reachable under root, plus per-level lists."""
    children_by_parent: dict[str, set[str]] = {}
    for name, parent in pairs:
        children_by_parent.setdefault(parent, set()).add(name)

    all_under: set[str] = set()
    frontier: set[str] = {root}
    levels: list[list[str]] = []

    while True:
        # Direct children of any name in the current frontier.
        next_level: list[str] = []
        for p in frontier:
            for child in children_by_parent.get(p, ()):
                if child not in all_under:
                    next_level.append(child)
        if not next_level:
            break
        next_level = sorted(set(next_level))
        levels.append(next_level)
        all_under.update(next_level)
        frontier = set(next_level)

    return all_under, levels


def resolve_root_names(
    pairs: list[tuple[str, str]], requested_roots: list[str]
) -> tuple[list[str], list[str]]:
    """Map requested roots to exact XML names using case-insensitive matching."""
    existing_names = {name for name, _ in pairs}
    by_normalized = {normalize_group_name(name): name for name in existing_names}
    resolved: list[str] = []
    missing: list[str] = []
    for requested in requested_roots:
        exact = (
            requested
            if requested in existing_names
            else by_normalized.get(normalize_group_name(requested))
        )
        if exact is None:
            missing.append(requested)
            continue
        resolved.append(exact)
    # Keep order stable but remove duplicates.
    seen: set[str] = set()
    ordered_unique = [name for name in resolved if not (name in seen or seen.add(name))]
    return ordered_unique, missing


def parent_names_from_roots(groups_xml: str, roots: list[str]) -> tuple[set[str], list[str]]:
    """Root names plus descendants under each root (used as ledger parent filter)."""
    pairs = load_parent_name_pairs(groups_xml)
    resolved_roots, missing_roots = resolve_root_names(pairs, roots)
    combined: set[str] = set()
    for root_name in resolved_roots:
        descendants, _ = collect_descendants(pairs, root_name)
        combined.update(descendants)
        combined.add(root_name)
    return combined, missing_roots


def load_group_names(path: str | None) -> set[str]:
    """Plain-text list: one group name per line; # starts a comment line."""
    if path is None or path == "-":
        lines = sys.stdin
    else:
        lines = open(path, encoding="utf-8")
    names: set[str] = set()
    try:
        for line in lines:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            names.add(s)
    finally:
        if path is not None and path != "-":
            lines.close()
    return names


def ledgers_with_parent_in(xml_path: str, parent_names: set[str]) -> list[str]:
    """Streaming parse of large ledgers file; elem.clear() limits memory use."""
    out: list[str] = []
    for _event, elem in ET.iterparse(xml_path, events=("end",)):
        if elem.tag != "LEDGER":
            continue
        parent_el = elem.find("PARENT")
        parent = (parent_el.text or "").strip()
        if parent not in parent_names:
            elem.clear()
            continue
        name = elem.get("NAME")
        if name:
            out.append(name.strip())
        elem.clear()
    return sorted(set(out))


def cmd_groups_only(groups_xml: str, roots: list[str], verbose: bool) -> int:
    """Emit sorted group names under selected roots; -v mirrors BFS depth on stderr."""
    pairs = load_parent_name_pairs(groups_xml)
    roots, missing_roots = resolve_root_names(pairs, roots)
    if missing_roots:
        print(
            f"exclude_groups_ledgers: root groups not found in XML: {', '.join(missing_roots)}",
            file=sys.stderr,
        )
    combined: set[str] = set()
    root_levels: list[tuple[str, list[list[str]]]] = []
    for root_name in roots:
        descendants, levels = collect_descendants(pairs, root_name)
        combined.update(descendants)
        combined.add(root_name)
        root_levels.append((root_name, levels))
    combined_sorted = sorted(combined)

    if verbose:
        for root_name, levels in root_levels:
            print(f"Level 0 (root): {root_name}", file=sys.stderr)
            for i, lvl in enumerate(levels, start=1):
                print(f"Level {i} ({len(lvl)} new):", file=sys.stderr)
                for n in lvl:
                    print(f"  {n}", file=sys.stderr)
            print(file=sys.stderr)
        print(
            f"Total unique names (including roots): {len(combined_sorted)}",
            file=sys.stderr,
        )

    for name in combined_sorted:
        print(name)
    return 0


def cmd_ledgers(
    groups_xml: str,
    ledgers_xml: str,
    roots: list[str],
    parent_list_path: str | None,
) -> int:
    # Parent set: explicit file > stdin pipe > auto closure from groups_xml.
    if parent_list_path is not None:
        parent_names = load_group_names(parent_list_path)
    elif not sys.stdin.isatty():
        parent_names = load_group_names(None)
    else:
        parent_names, missing_roots = parent_names_from_roots(groups_xml, roots)
        if missing_roots:
            print(
                f"exclude_groups_ledgers: root groups not found in XML: {', '.join(missing_roots)}",
                file=sys.stderr,
            )

    if not parent_names:
        print("exclude_groups_ledgers: no parent group names loaded", file=sys.stderr)
        return 1

    for n in ledgers_with_parent_in(ledgers_xml, parent_names):
        print(n)
    return 0


def main() -> int:
    # See module docstring; data_file meaning depends on --groups-only.
    parser = argparse.ArgumentParser(
        description=(
            "Group closure utility: print group closure (--groups-only), or print ledgers "
            "whose PARENT is in that group set (default)."
        )
    )
    parser.add_argument(
        "-G",
        "--groups-only",
        action="store_true",
        help="Print group names under selected roots only (no ledger scan).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="With --groups-only: print each BFS level on stderr.",
    )
    parser.add_argument(
        "--roots",
        nargs="+",
        default=list(DEFAULT_ROOT_GROUPS),
        metavar="GROUP",
        help=(
            "Root group names to expand (default: Duties & Taxes, Cash-in-Hand, "
            "Bank Accounts, Branch / Divisions). Add more names for future groups."
        ),
    )
    parser.add_argument(
        "--groups-xml",
        default="tally_groups_final.xml",
        metavar="PATH",
        help="Tally groups XML for the closure (default: ./tally_groups_final.xml).",
    )
    parser.add_argument(
        "--ledgers-xml",
        default="tally_ledgers_final.xml",
        metavar="PATH",
        help="Tally ledgers XML to scan (default: ./tally_ledgers_final.xml).",
    )
    parser.add_argument(
        "data_file",
        nargs="?",
        default=None,
        metavar="FILE",
        help=(
            "With --groups-only: optional path to groups XML (overrides --groups-xml). "
            "Without --groups-only: optional file of parent group names (one per line); "
            "if omitted, use stdin when piped, else closure from --roots."
        ),
    )
    args = parser.parse_args()

    if args.groups_only:
        groups_xml = args.data_file or args.groups_xml
        return cmd_groups_only(groups_xml, args.roots, args.verbose)

    groups_xml = args.groups_xml
    return cmd_ledgers(groups_xml, args.ledgers_xml, args.roots, args.data_file)


if __name__ == "__main__":
    raise SystemExit(main())
