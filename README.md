# TALLY_EXPORT

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)
![Platform](https://img.shields.io/badge/Platform-Tally%20Prime%20%2F%20ERP%209-orange)
![Format](https://img.shields.io/badge/Output-XML%20%7C%20JSON-green)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

> **Python ETL toolkit for Tally Prime / Tally.ERP 9** — export groups, ledgers, and vouchers over the built-in XML HTTP API, then run rich offline analyses: ledger classification, tax-group closure, voucher pattern detection, and per-ledger JSON slicing.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites & Installation](#prerequisites--installation)
- [Quick Start](#quick-start)
- [Script Reference](#script-reference)
  - [0. run.py](#0-runpy--pipeline-orchestrator)
  - [1. tally_groups.py](#1-tally_groupspy--group-master--classification)
  - [2. tally_ledger_master.py](#2-tally_ledger_masterpy--ledger-master--enrichment)
  - [3. tally_daybook.py](#3-tally_daybookpy--voucher-register)
  - [4. list_liability_ledgers.py](#4-list_liability_ledgerspy)
  - [5. list_expense_ledgers.py](#5-list_expense_ledgerspy)
  - [6. groups.py](#6-groupspy--exclude-groups-closure)
  - [7. detect_cross_vouchers.py](#7-detect_cross_voucherspy--cross-voucher-pattern)
  - [8. final_list.py](#8-final_listpy--voucher-pattern-ledgers-minus-duties--taxes)
  - [9. split_by_ledger.py](#9-split_by_ledgerpy--per-ledger-daybook-slices)
  - [10. to_json.py](#10-to_jsonpy--json-with-ledger-master)
  - [11. apply_expense_blocklist.py](#11-apply_expense_blocklistpy--llm-blocklist-filter-tds-mode)
  - [12. tds_expense_wrapper.py](#12-tds_expense_wrapperpy--end-to-end-tds-orchestrator)
  - [13. expense_blocklist_categories.json](#13-expense_blocklist_categoriesjson--blocklist-config)
- [TDS Analysis Mode](#tds-analysis-mode)
- [Data File Reference](#data-file-reference)
- [Output Folder Structure](#output-folder-structure)
- [Key Design Patterns](#key-design-patterns)
- [Classification System](#classification-system)
- [JSON Output Structure](#json-output-structure)
- [What You Get (Example Scale)](#what-you-get-example-scale)
- [Troubleshooting](#troubleshooting)
- [Notes & Caveats](#notes--caveats)
- [License](#license)

---

## Overview

Tally Prime and Tally.ERP 9 expose an **XML HTTP API** on `localhost:9000` that can return group hierarchies, ledger masters, and voucher registers in XML format. This toolkit wraps that API with:

- **Export scripts** that pull data from a live Tally instance and write structured XML to disk.
- **Offline analysis scripts** that operate purely on those XML files — no Tally required — to classify ledgers, close the *Duties & Taxes* group tree, detect cross-entry voucher patterns, and produce per-ledger daybook slices with enriched JSON.

The entire pipeline is designed for **any Tally company and any date range**. Simply point the scripts at your Tally instance and the date range you need.

---

## Features

- **Complete group export** with automatic classification into `NATURE` (Asset / Liability / Income / Expense), `ROOTPRIMARY` (top-level group), and `FINANCIALSTATEMENT` (Balance Sheet / P&L).
- **Full ledger master export** with wide FETCH spec (GST, mailing, bank, tax details) and optional enrichment from the group hierarchy.
- **Chunked daybook export** — month-by-month to stay within Tally's HTTP timeout limits; deduplicates by GUID across chunks.
- **Ledger deduplication & merging** — normalises whitespace, merges duplicate ledger names with union semantics across scalar and list fields.
- **Streaming XML parsing** via `iterparse` + `elem.clear()` — handles multi-hundred-MB daybook files without loading the entire document into memory.
- **Exclude-groups BFS closure** — walks the group tree to collect all sub-groups of selected root groups (default: *Duties & Taxes*, *Cash-in-Hand*, *Bank Accounts*, *Branch / Divisions*), then lists ledgers whose parent sits in that set.
- **Cross-voucher pattern detection** — finds vouchers that simultaneously debit an expense/fixed-asset ledger and credit a liability/current-asset ledger.
- **Per-ledger daybook slicing** — one XML file per target ledger containing every voucher that references it.
- **JSON export with canonical field resolution** — resolves GST, PAN, state, pincode from multiple Tally storage locations; records `field_sources` for auditability.
- **Safe filename generation** — sanitises ledger names to valid filesystem paths with collision handling.

---

## Architecture

```mermaid
flowchart TB
    subgraph tally["🏢 Tally (localhost:9000)"]
        API["XML HTTP API"]
    end

    subgraph export["📤 export/ — live Tally required"]
        TG["tally_groups.py"]
        TL["tally_ledger_master.py"]
        TD["tally_daybook.py"]
    end

    subgraph files["💾 Generated XML / JSON files"]
        GF["tally_groups_final.xml\n(group hierarchy + classification)"]
        LF["tally_ledgers_final.xml\n(enriched ledger master)"]
        DB["daybook_DDMMYYYY_to_DDMMYYYY.xml\n(voucher register)"]
        VBL["vouchers_by_final_list/\n*.xml — one per ledger"]
        JF["vouchers_by_final_list_json/\n*.json — one per ledger"]
    end

    subgraph offline["🔍 Offline analysis — XML only, no Tally needed"]
        LL["analyze/list_liability_ledgers.py"]
        LE["analyze/list_expense_ledgers.py"]
        DT["core/groups.py"]
        VX["analyze/detect_cross_vouchers.py"]
        FL["analyze/final_list.py"]
        SP["output/split_by_ledger.py"]
        VJ["output/to_json.py"]
    end

    subgraph tds["🧮 TDS Analysis Mode (optional, requires Claude API key)"]
        BC["expense_blocklist_categories.json\n(11 PDF categories)"]
        AB["tds/apply_expense_blocklist.py\n(LLM filter + audit report)"]
        TW["tds/tds_expense_wrapper.py\n(end-to-end TDS pipeline)"]
        EF["expense_filtered.json"]
        AR["expense_blocklist_report.json"]
    end

    API -->|"XML POST"| TG
    API -->|"XML POST"| TL
    API -->|"XML POST\n(month chunks)"| TD

    TG --> GF
    TL --> LF
    TD --> DB

    GF --> DT
    LF --> LL
    LF --> LE
    LF --> DT
    LF --> VX
    DB --> VX
    GF --> FL
    LF --> FL
    DB --> FL
    GF --> SP
    LF --> SP
    DB --> SP
    SP --> VBL
    LF --> VJ
    VBL --> VJ
    VJ --> JF

    LF --> AB
    BC --> AB
    AB --> EF
    AB --> AR
    EF -.->|"--filtered-expense"| FL
    LF --> TW
    DB --> TW
    GF --> TW
    BC --> TW
    TW --> EF
    TW --> AR
```

---

## Prerequisites & Installation

### Tally setup

1. Open your company in **Tally Prime** or **Tally.ERP 9**.
2. Go to **F12 Configure → Advanced Configuration** (or **Gateway of Tally → F12**).
3. Enable **"Allow TDL XML HTTP API"** (or **"Enable HTTP server"**) and note the port (default **9000**).
4. Tally must remain open and listening while export scripts run.

### Python environment

```bash
# Python 3.10+ required (uses X | Y type-union syntax)
python --version

# Install runtime dependencies (anthropic only needed for TDS Analysis Mode)
pip install -r requirements.txt
# or, if you don't need TDS mode:
pip install requests
```

### Get the code

```bash
git clone https://github.com/<your-username>/TALLY_EXPORT.git
cd TALLY_EXPORT
```

### Optional: Claude API key (only for TDS Analysis Mode)

The TDS workflow uses Claude to identify ledgers that should be excluded from
TDS analysis (discounts, GST components, statutory penalties, etc. — see the
[TDS Analysis Mode](#tds-analysis-mode) section below). You can skip this if
you only need the default voucher-pattern pipeline.

```bash
cp config/.env.example .env
# edit .env and paste your real ANTHROPIC_API_KEY
# get one at https://console.anthropic.com/

# the scripts read ANTHROPIC_API_KEY from the environment
export $(grep -v '^#' .env | xargs)
```

---

## Quick Start

**One command runs the full pipeline** from the repo root:

```bash
python run.py
```

`run.py` wires all phases together, passes the correct file paths automatically,
and streams progress to the terminal. All generated files land in `data/`.

### Common invocations

| Command | What it does |
|---------|-------------|
| `python run.py` | Standard offline pipeline — compute ledger list → split daybook → convert to JSON |
| `python run.py --tds` | TDS mode — LLM blocklist filter before voucher scan (needs `ANTHROPIC_API_KEY`) |
| `python run.py --export` | Export from live Tally first, then run offline pipeline |
| `python run.py --export --start 01-04-2024 --end 31-03-2025` | Export with explicit date range, then pipeline |
| `python run.py --export --tds` | Export from Tally + TDS mode in one shot |

After a standard run you have (all inside `data/`):
- `data/final.txt` — sorted target ledger list
- `data/vouchers_by_final_list/` — one XML per target ledger
- `data/vouchers_by_final_list_json/` — one JSON per target ledger

After `--tds` you additionally get:
- `data/expense_filtered.json` — expense ledgers with blocklisted names removed
- `data/expense_blocklist_report.json` — per-name LLM audit report

See [`run.py`](#0-runpy--pipeline-orchestrator) in Script Reference and
[TDS Analysis Mode](#tds-analysis-mode) for full details.

---

## Script Reference

### 0. `run.py` — Pipeline orchestrator

**Role:** Single entry point that runs the entire pipeline in the correct order.
Resolves all file paths automatically (everything reads from and writes to `data/`).
No `PYTHONPATH` setup needed — each script inserts the project root into `sys.path`
at startup so package imports resolve correctly.

**CLI flags:**

| Flag | Default | Effect |
|---|---|---|
| `--export` | off | Run Tally export phase first (Tally must be open on `localhost:9000`) |
| `--start DD-MM-YYYY` | `01-04-2024` | Daybook start date (used with `--export`) |
| `--end DD-MM-YYYY` | `31-03-2025` | Daybook end date (used with `--export`) |
| `--tds` | off | Apply LLM expense blocklist before voucher scan (TDS mode) |

**Run:**

```bash
# Standard offline pipeline (XMLs already in data/)
python run.py

# TDS mode
python run.py --tds

# Export from Tally first, then offline pipeline
python run.py --export
python run.py --export --start 01-04-2024 --end 31-03-2025

# Export from Tally + TDS mode in one shot
python run.py --export --tds --start 01-04-2024 --end 31-03-2025
```

---

### 1. `tally_groups.py` — Group master + classification

**Role:** Fetches all Tally groups via a Collection XML request, walks each group's parent chain to its root primary group, and writes `tally_groups_final.xml` with enrichment tags added.

| Field added | Meaning |
|---|---|
| `ROOTPRIMARY` | Top-level ancestor (e.g. `Current Liabilities`, `Indirect Expenses`) |
| `NATURE` | `Asset` / `Liability` / `Income` / `Expense` / `Primary` |
| `FINANCIALSTATEMENT` | `Balance Sheet` / `P&L` / `Root` |

**Output:** `tally_groups_final.xml` — root `<TALLYGROUPS>`, children `<GROUP>`.

**Run:**

```bash
python export/tally_groups.py
```

> ⚠️ **Warning:** This script executes at import time (no `if __name__ == "__main__"` guard). Do **not** `import tally_groups` as a library unless you intend to trigger a live HTTP request.

**Downstream consumers:** `core/groups.py`, `analyze/final_list.py`, `output/split_by_ledger.py`.

---

### 2. `tally_ledger_master.py` — Ledger master + enrichment

**Role:** Fetches all ledgers with a wide `<FETCH>` spec (GST, mailing, bank, income-tax details), optionally merges duplicate names, optionally enriches with `ROOTPRIMARY` / `NATURE` / `FINANCIALSTATEMENT`, and writes `tally_ledgers_final.xml`.

**Default output:** `tally_ledgers_final.xml` — root `<TALLYLEDGERS>`, children `<LEDGER>` in native Tally subtree format.

**CLI flags:**

| Flag | Default | Effect |
|---|---|---|
| `--out PATH` | `tally_ledgers_final.xml` | Override output path |
| `--no-enrich` | enrichment on | Skip adding ROOTPRIMARY / NATURE |
| `--legacy-flat` | off | Use flat field list instead of native subtrees |
| `--beautify` | on | Strip `TYPE` attributes from XML |

**Run:**

```bash
python export/tally_ledger_master.py
python export/tally_ledger_master.py --out data/tally_ledgers_final.xml
python export/tally_ledger_master.py --no-enrich
python export/tally_ledger_master.py --legacy-flat
```

**Programmatic API:**

```python
from export.tally_ledger_master import export_ledgers_to_path
export_ledgers_to_path("my_ledgers.xml")
```

**Downstream consumers:** All offline analysis scripts.

---

### 3. `tally_daybook.py` — Voucher register

**Role:** Exports vouchers for a given date range by fetching **one calendar month at a time** (to avoid Tally HTTP timeouts). Deduplicates by `GUID` across chunks. Normalises the XML to `<TALLYDAYBOOK>` / `<VOUCHER>` / `<LEDGERENTRIES>` / `<ENTRY>`.

**Output:** `daybook_DDMMYYYY_to_DDMMYYYY.xml` (auto-named from date range, or `--out`).

**CLI flags:**

| Flag | Default | Effect |
|---|---|---|
| `--start DD-MM-YYYY` | required | First date of range |
| `--end DD-MM-YYYY` | required | Last date of range |
| `--out PATH` | auto from dates | Override output path |

**Run:**

```bash
python export/tally_daybook.py --start 01-04-2024 --end 31-03-2025
python export/tally_daybook.py --start 01-04-2024 --end 31-03-2025 --out data/daybook_01042024_to_31032025.xml
```

**Programmatic API:**

```python
from export.tally_daybook import export_daybook_to_path
export_daybook_to_path("01-04-2024", "31-03-2025", "my_daybook.xml")
```

> **Timeouts:** 900 s read timeout per monthly chunk. Very large companies may still need smaller ranges or TDL-level optimisation.

**Downstream consumers:** `analyze/detect_cross_vouchers.py`, `analyze/final_list.py`, `output/split_by_ledger.py`.

---

### 4. `list_liability_ledgers.py`

**Location:** `analyze/list_liability_ledgers.py`

**Role:** Reads the enriched ledger XML and prints one ledger name per line for every ledger where:

- `NATURE == "Liability"`, **or**
- `ROOTPRIMARY == "Current Assets"`

Includes only names that also appear in at least one daybook voucher entry.

**Run:**

```bash
python analyze/list_liability_ledgers.py data/tally_ledgers_final.xml data/daybook_01042024_to_31032025.xml
```

Uses `iterparse` — safe for large ledger files.

---

### 5. `list_expense_ledgers.py`

**Location:** `analyze/list_expense_ledgers.py`

**Role:** Same enrichment contract as script 4. Includes a ledger if:

- `NATURE == "Expense"`, **or**
- `ROOTPRIMARY == "Fixed Assets"`

**CLI flags:**

| Flag | Default | Effect |
|---|---|---|
| `--xml PATH` | `tally_ledgers_final.xml` (next to script) | Ledger master path |
| `--daybook PATH` | `daybook_01042024_to_31032025.xml` (next to script) | Daybook path |
| `--json` | off | Output as JSON array instead of one-per-line |

**Run:**

```bash
python analyze/list_expense_ledgers.py --xml data/tally_ledgers_final.xml --daybook data/daybook_01042024_to_31032025.xml
python analyze/list_expense_ledgers.py --xml data/tally_ledgers_final.xml --daybook data/daybook_01042024_to_31032025.xml --json
```

---

### 6. `groups.py` — Exclude-groups closure

**Location:** `core/groups.py`

**Role:**

1. Reads `tally_groups_final.xml` and performs a **BFS** over parent → child links to collect the full set of groups under selected root groups (default: **`Duties & Taxes`**, **`Cash-in-Hand`**, **`Bank Accounts`**, **`Branch / Divisions`**).
2. Reads `tally_ledgers_final.xml` and lists every ledger whose `PARENT` field appears in that group set.

**CLI flags:**

| Flag | Default | Effect |
|---|---|---|
| `--groups-xml PATH` | `tally_groups_final.xml` | Group hierarchy file |
| `--ledgers-xml PATH` | `tally_ledgers_final.xml` | Ledger master file |
| `--groups-only` | off | Print group names only (no ledgers) |
| `-v` | off | Verbose BFS level debugging |
| `--roots GROUP [GROUP ...]` | default root list | Override / extend root groups to include in closure |

**Run:**

```bash
python core/groups.py
python core/groups.py --groups-only -v
python core/groups.py --groups-xml data/tally_groups_final.xml --ledgers-xml data/tally_ledgers_final.xml
python core/groups.py --roots "Duties & Taxes" "Cash-in-Hand" "Bank Accounts" "Branch / Divisions"
```

**Downstream consumers:** `analyze/final_list.py`, `output/split_by_ledger.py`, `tds/tds_expense_wrapper.py` (all import `parent_names_from_roots` and `ledgers_with_parent_in` from this module).

---

### 7. `detect_cross_vouchers.py` — Cross-voucher pattern

**Location:** `analyze/detect_cross_vouchers.py`

**Role:** Detects vouchers that match **both** of these conditions simultaneously:

1. At least one entry credits a **liability / current-asset** ledger (`ISDEEMEDPOSITIVE == "No"`)
2. At least one entry debits an **expense / fixed-asset** ledger (`ISDEEMEDPOSITIVE == "Yes"`)

Prints the **distinct liability/current-asset ledger names** from all matching vouchers, sorted.

**CLI flags:**

| Flag | Default | Effect |
|---|---|---|
| `--ledgers PATH` | `tally_ledgers_final.xml` (next to script) | Enriched ledger master |
| `--daybook PATH` | `daybook_01042024_to_31032025.xml` (next to script) | Daybook XML |
| `--json` | off | Output as JSON array |
| `-o PATH` | `test.txt` (next to script) | Write output to file instead of stdout |
| `--filtered-expense PATH` | (none) | **Explicit TDS override:** load the expense_or_fixed set from this file (JSON array or one-name-per-line text). Beats auto-detect. |
| `--no-filter` | off | Force raw-XML classification even if `expense_filtered.json` exists next to the ledgers XML. Use for non-TDS runs. |

**Run:**

```bash
python analyze/detect_cross_vouchers.py \
  --ledgers data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml
python analyze/detect_cross_vouchers.py \
  --ledgers data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml --json

# TDS mode — explicit override
python analyze/detect_cross_vouchers.py \
  --ledgers data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml \
  --filtered-expense data/expense_filtered.json

# Force raw XML even if a sidecar exists
python analyze/detect_cross_vouchers.py \
  --ledgers data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml --no-filter
```

**Architecture note — single source of truth + auto-detect.** Every other
script in the pipeline that needs an expense set (`analyze/final_list.py`,
`output/split_by_ledger.py`, `tds/tds_expense_wrapper.py`) imports
`load_expense_and_liability_sets()` from `core/ledger_sets.py`. The override
hook lives inside that function, so `--filtered-expense`, `--no-filter`, and the
auto-detect behavior all work the same way everywhere — there's exactly one
piece of code that knows how to pick the expense source.

**Auto-detect: how the expense source is chosen** (resolution order):

| Priority | Condition | Source |
|---|---|---|
| 1 | `--filtered-expense FILE` is passed | The given file |
| 2 | `--no-filter` is passed | Raw XML extraction |
| 3 | `expense_filtered.json` exists next to the ledgers XML | The sidecar (auto-detected) |
| 4 | None of the above | Raw XML extraction |

Every run logs the resolved source on stderr (e.g.
`[core.ledger_sets] Auto-detected filter sidecar: expense_filtered.json (next to ledgers XML).`)
so the audit trail is in stdout/stderr regardless of how the script was invoked.

**Staleness check.** If the auto-detected sidecar is older than the ledgers
XML (i.e. you re-exported from Tally but didn't re-run `apply_expense_blocklist.py`),
a loud `WARNING` line is printed but the run continues using the existing
sidecar. Re-run `apply_expense_blocklist.py` to refresh. Pass `--no-filter` to
ignore the sidecar entirely.

**Downstream consumers:** `analyze/final_list.py`, `tds/tds_expense_wrapper.py` (both import
`collect_matching_liability_names` from this module).

---

### 8. `final_list.py` — Voucher-pattern ledgers minus excluded groups

**Location:** `analyze/final_list.py`

**Role:** Produces the **set difference**:

```
voucher_pattern_ledgers  −  excluded_group_ledgers
```

Imports `collect_matching_liability_names` from `analyze/detect_cross_vouchers.py` and
`parent_names_from_roots` / `ledgers_with_parent_in` from `core/groups.py` — no shell pipe required.

**CLI flags:**

| Flag | Default | Effect |
|---|---|---|
| `--ledgers PATH` | `tally_ledgers_final.xml` (next to script) | Ledger master |
| `--daybook PATH` | `daybook_01042024_to_31032025.xml` (next to script) | Daybook XML |
| `--groups-xml PATH` | `tally_groups_final.xml` (next to script) | Group hierarchy |
| `--filtered-expense PATH` | (none) | **Explicit TDS override:** load the expense_or_fixed set from this file. Beats auto-detect. |
| `--no-filter` | off | Force raw-XML classification even if `expense_filtered.json` exists next to the ledgers XML. Use for non-TDS runs while a sidecar is present. |
| `--json` | off | Output as JSON array |

**Run:**

```bash
python analyze/final_list.py \
  --ledgers data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml \
  --groups-xml data/tally_groups_final.xml
python analyze/final_list.py \
  --ledgers data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml \
  --groups-xml data/tally_groups_final.xml --json

# Save to file
python analyze/final_list.py \
  --ledgers data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml \
  --groups-xml data/tally_groups_final.xml > data/final.txt

# TDS workflow: apply the blocklist, review the report, then run final_list — auto-detect picks up the sidecar
python tds/apply_expense_blocklist.py --input expense_raw.json \
  --output data/expense_filtered.json --report data/expense_blocklist_report.json
# (review data/expense_blocklist_report.json)
python analyze/final_list.py \
  --ledgers data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml \
  --groups-xml data/tally_groups_final.xml  # auto-detects expense_filtered.json next to the ledgers XML
```

**Downstream consumers:** `output/split_by_ledger.py` imports `load_final_ledger_names()` from this module directly.

---

### 9. `split_by_ledger.py` — Per-ledger daybook slices

**Location:** `output/split_by_ledger.py`

**Role:** Calls `load_final_ledger_names()` from `analyze/final_list.py`, scans the daybook **once**, and writes **one `<TALLYDAYBOOK>` XML file per ledger name** containing every voucher that references that ledger (by `LEDGERNAME` or `PARTYLEDGERNAME`).

**Output:** `vouchers_by_final_list/` folder — one `.xml` per ledger.  
Each file root carries `FROMDATE`, `TODATE`, and `TOTALCOUNT` attributes.  
Filenames are **sanitised** (`<>:"/\|?*` → `_`) with `_2`, `_3` suffixes on collision.

**CLI flags:**

| Flag | Default | Effect |
|---|---|---|
| `--ledgers PATH` | `tally_ledgers_final.xml` (next to script) | Ledger master |
| `--daybook PATH` | `daybook_01042024_to_31032025.xml` (next to script) | Daybook XML |
| `--groups-xml PATH` | `tally_groups_final.xml` (next to script) | Group hierarchy |
| `--out-dir PATH` | `vouchers_by_final_list` (next to script) | Output folder |
| `--filtered-expense PATH` | (none) | **Explicit TDS override:** load the expense_or_fixed set from this file. Beats auto-detect. |
| `--no-filter` | off | Force raw-XML classification even if `expense_filtered.json` is auto-detected next to the ledgers XML. |

**Run:**

```bash
python output/split_by_ledger.py \
  --ledgers data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml \
  --groups-xml data/tally_groups_final.xml \
  --out-dir data/vouchers_by_final_list

# TDS mode — auto-detects expense_filtered.json next to the ledgers XML
python output/split_by_ledger.py \
  --ledgers data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml \
  --groups-xml data/tally_groups_final.xml \
  --out-dir data/vouchers_by_final_list

# Force raw XML even if a sidecar is present
python output/split_by_ledger.py \
  --ledgers data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml \
  --groups-xml data/tally_groups_final.xml \
  --out-dir data/vouchers_by_final_list --no-filter
```

**Downstream consumers:** `output/to_json.py`.

---

### 10. `to_json.py` — JSON with ledger master

**Location:** `output/to_json.py`

**Role:** Converts each `vouchers_by_final_list/*.xml` to a structured JSON file. Parses `tally_ledgers_final.xml` **once** and resolves canonical values for GST, PAN, address, and registration from multiple Tally storage locations.

**JSON output per file:**

```
{
  "ledger_master": { ... resolved fields + field_sources ... },
  "daybook":       { ... vouchers as JSON ... }
}
```

**Field resolution priority:**

| Field | Priority order |
|---|---|
| `GSTIN` | `PARTYGSTIN` → `LEDGSTREGDETAILS.LIST[n].GSTIN` |
| `PAN` | `INCOMETAXNUMBER` from multiple blocks (all distinct recorded) |
| `STATE` | `PRIORSTATENAME` → `LEDMAILINGDETAILS` → `LEDGSTREGDETAILS` |
| `PINCODE` | Direct `PINCODE` → `LEDMAILINGDETAILS.PINCODE` |

When Tally stores **multiple distinct values** for the same concept, the script emits `GSTIN_all_distinct` / `PAN_all_distinct` alongside the canonical choice.

**CLI flags:**

| Flag | Default | Effect |
|---|---|---|
| `--ledgers PATH` | `tally_ledgers_final.xml` (next to script) | Ledger master |
| `--vouchers-dir PATH` | `vouchers_by_final_list` (next to script) | Input XML folder |
| `--output-dir PATH` | `vouchers_by_final_list_json` (next to script) | Output JSON folder |
| `--dry-run` | off | Parse only, do not write files |

**Run:**

```bash
python output/to_json.py \
  --ledgers data/tally_ledgers_final.xml \
  --vouchers-dir data/vouchers_by_final_list \
  --output-dir data/vouchers_by_final_list_json
python output/to_json.py \
  --ledgers data/tally_ledgers_final.xml \
  --vouchers-dir data/vouchers_by_final_list \
  --output-dir data/vouchers_by_final_list_json --dry-run
```

> **Note:** If a file stem does not match any `<LEDGER NAME="...">` in the master (e.g. after a ledger rename), `ledger_master` will include a `_lookup_error` key instead of resolved fields.

---

### 11. `apply_expense_blocklist.py` — LLM blocklist filter (TDS mode)

**Location:** `tds/apply_expense_blocklist.py`

**Role:** Reads a list of ledger names (the output of
`analyze/list_expense_ledgers.py`, or any JSON array / one-name-per-line
text file) and uses Claude (Anthropic API) to identify which names fall under
any of the 11 blocklist categories defined in `expense_blocklist_categories.json`
(discount, round-off, bad debts, P&L on sale of asset, prior period, write-off,
bank charges, late fees & penalties, GST, income tax, ESI/PF). Writes a
filtered list (input minus blocklisted) plus a per-ledger audit report.

**Why pure-LLM, not regex:** the PDF rules carry explicit nuances that defeat
keyword matching — `purchase-GST` is not a GST blocklist (it's a purchase
ledger), `Tax Audit Fees` is not Income Tax (it's professional fees, TDS u/s
194J), `Interest paid to Vendor X` is not statutory (TDS u/s 194A). Sending
every name through Claude with the full PDF intents in the system prompt
applies these nuances uniformly.

**Safeguards:**

- `claude-opus-4-7` with adaptive thinking — the model reasons through edge cases
- Forced tool use with strict JSON schema — model cannot drift to free-form output
- Persistent JSON cache (`expense_blocklist_cache.json`) — re-runs are byte-identical and free
- Prompt caching on the system prompt — second batch onward is much cheaper
- Audit report has `name | blocklisted | category | reason | source` for every input
- Default bias toward keep — when ambiguous, the prompt explicitly instructs `blocklisted=false`
- Defensive guard — if the model returns an invalid category, the script overrides to keep

**CLI flags:**

| Flag | Default | Effect |
|---|---|---|
| `--input PATH` | required | JSON array or one-name-per-line text file of ledger names |
| `--config PATH` | `expense_blocklist_categories.json` (next to script) | Categories config |
| `--output PATH` | `expense_filtered.json` (next to script) | Filtered list (input minus blocklisted) |
| `--report PATH` | `expense_blocklist_report.json` (next to script) | Per-name audit report |
| `--cache PATH` | `expense_blocklist_cache.json` (next to script) | Persistent decision cache |
| `--model NAME` | `claude-opus-4-7` | Anthropic model ID |
| `--batch-size N` | 25 | Names per LLM call |
| `--max-tokens N` | 32000 | Output cap per batch (streaming used) |
| `--text` | off | Write `--output` as one name per line instead of JSON array |
| `--dry-run` | off | Write only the audit report; do not write the filtered list |

**Run:**

```bash
# 1. Produce the raw expense list
python analyze/list_expense_ledgers.py \
  --xml data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml \
  --json > expense_raw.json

# 2. First time: dry-run, review the report
python tds/apply_expense_blocklist.py --input expense_raw.json \
  --config config/expense_blocklist_categories.json --dry-run

# 3. Inspect expense_blocklist_report.json — names containing 'tax' or 'gst'
#    deserve special attention. Confirm Tax Audit Fees / purchase-GST / interest-on-loan
#    are all marked blocklisted=false.

# 4. Run for real (cache makes this byte-identical and free on later runs)
python tds/apply_expense_blocklist.py --input expense_raw.json \
  --config config/expense_blocklist_categories.json \
  --output data/expense_filtered.json \
  --report data/expense_blocklist_report.json
```

**Cheap / fast mode (no per-name reasoning):**

```bash
# ~$0.05–0.10, ~30–60 seconds for ~1500 ledgers
python tds/apply_expense_blocklist.py --input expense_raw.json \
    --config config/expense_blocklist_categories.json \
    --output data/expense_filtered.json \
    --model claude-haiku-4-5 \
    --no-thinking \
    --no-reasons \
    --batch-size 100 \
    --concurrency 5
```

The cheap-mode flags trade per-name LLM reasoning for ~50–100× lower cost and
much faster wall-clock time:

| Flag | Default | Effect |
|---|---|---|
| `--no-thinking` | off | Disable adaptive thinking. Required for Haiku 4.5 / Sonnet 4.5; saves the bulk of the output cost on Opus / Sonnet 4.6. |
| `--no-reasons` | off | Drop the per-name `reason` field from the LLM tool schema. Audit report falls back to a synthesized category-level reason (the PDF intent text). Cuts decision tokens ~5×. |
| `--concurrency N` | 1 | Run N batches in parallel via `ThreadPoolExecutor`. Linear wall-clock speedup until you hit Anthropic rate limits — try 5. |

When `--no-reasons` is set, each report entry looks like:
```json
{"name": "Bank Charges - HDFC", "blocklisted": true, "category": 7,
 "reason": "PDF cat 7: Ledgers that record fees levied directly by banks..."}
```
You still know *what* got blocked and *which category*; you just lose the
model's per-name justification. The category nuances (purchase-GST not blocked,
Tax Audit Fees not blocked, vendor interest not blocked) are still respected
because the system prompt still contains the full intent text.

**Environment:** Requires `ANTHROPIC_API_KEY` (see [Prerequisites](#prerequisites--installation)).

**Library use:** `from tds.apply_expense_blocklist import filter_names, load_config` — `filter_names` takes a list of names and returns `(kept_names, audit_report)`.

**Downstream consumers:** `analyze/final_list.py --filtered-expense ...`, `tds/tds_expense_wrapper.py` (uses `filter_names` internally).

---

### 12. `tds_expense_wrapper.py` — End-to-end TDS orchestrator

**Location:** `tds/tds_expense_wrapper.py`

**Role:** One-command end-to-end pipeline for TDS analysis. Combines:
1. Ledger classification via `core/ledger_sets.py` (`load_expense_and_liability_sets`)
2. LLM expense blocklist filter via `tds/apply_expense_blocklist.py` (`filter_names`)
3. Voucher scan via `analyze/detect_cross_vouchers.py` (`collect_matching_liability_names`)
4. Duties/cash/bank/branch group exclusion via `core/groups.py` (`parent_names_from_roots`, `ledgers_with_parent_in`)

Produces the same kind of output `analyze/final_list.py` produces — a sorted list of
liability/current-asset ledger names suitable for `output/split_by_ledger.py`
— but with TDS-irrelevant ledgers stripped before the voucher scan.

**CLI flags:**

| Flag | Default | Effect |
|---|---|---|
| `--ledgers PATH` | `tally_ledgers_final.xml` (next to script) | Ledger master |
| `--daybook PATH` | `daybook_01042024_to_31032025.xml` (next to script) | Daybook XML |
| `--groups-xml PATH` | `tally_groups_final.xml` (next to script) | Group hierarchy (for Stage 4 exclusion) |
| `--config PATH` | `expense_blocklist_categories.json` (next to script) | Blocklist categories |
| `--output PATH` | `test_filtered.txt` (next to script) | Final ledger-name list (sorted) |
| `--report PATH` | `expense_blocklist_report.json` (next to script) | LLM audit report |
| `--filtered-expense PATH` | `expense_filtered.json` (next to script) | Intermediate: blocklisted expense set (for diffing) |
| `--cache PATH` | `expense_blocklist_cache.json` (next to script) | Persistent LLM cache |
| `--model NAME` | `claude-opus-4-7` | Anthropic model ID |
| `--batch-size N` | 25 | Names per LLM call |
| `--max-tokens N` | 32000 | Output cap per batch |
| `--json` | off | Write `--output` as a sorted JSON array |
| `--no-group-exclusion` | off | Skip Stage 4 (duties/cash/bank/branch exclusion) |

**Run:**

```bash
python tds/tds_expense_wrapper.py \
  --ledgers data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml \
  --groups-xml data/tally_groups_final.xml \
  --config config/expense_blocklist_categories.json
```

**Environment:** Requires `ANTHROPIC_API_KEY`.

**Downstream consumers:** `output/split_by_ledger.py` (consumes the final ledger list).

---

### 13. `expense_blocklist_categories.json` — Blocklist config

A static JSON file transcribing the 11 PDF blocklist categories (intent text +
reference keywords). Read by both `tds/apply_expense_blocklist.py` and
`tds/tds_expense_wrapper.py`. Edit this file to adjust the category definitions or
keywords. The file is the single source of truth — both the LLM system prompt
and the audit report's category numbers are derived from it.

**Schema:**

```json
[
  {
    "id": 1,
    "name": "Discount allowed and received",
    "intent": "Ledgers that record trade or cash discounts ... No TDS applies because no service is being rendered — it's simply a price reduction.",
    "keywords": ["discount allowed", "discount received", ...]
  },
  ...
]
```

The `keywords` array is illustrative — the LLM judges by intent, not keyword
match. The keywords are surfaced in the system prompt to anchor the model's
understanding of each category.

---

## TDS Analysis Mode

This is an optional second pipeline that produces a TDS-filtered final ledger
list. Use it when the downstream JSON will feed a TDS analysis under the
Indian Income Tax Act and you want to exclude ledgers that are technically
"Expense" in Tally but not TDS-relevant (discounts, GST components, statutory
penalties, ESI/PF, etc.).

### What gets blocklisted

The 11 categories from `expense_blocklist_categories.json`:

| # | Category | Why excluded |
|---|---|---|
| 1 | Discount allowed/received | Contra-revenue adjustment, no service rendered |
| 2 | Round off | Mathematical balancing, no payee |
| 3 | Bad debts & provision | Internal write-off, no payment |
| 4 | P&L on sale of asset | Notional accounting entry, no payee |
| 5 | Prior period expense | TDS attached to the original (earlier) period |
| 6 | Write-off ledgers | Internal adjustment, no external party |
| 7 | Bank charges | Bank fees aren't TDS deductee payments |
| 8 | Late fees & penalties | Statutory dues outside TDS framework |
| 9 | GST ledgers in expenses | Tax to government, not vendor payment |
| 10 | Income tax | Direct tax to government |
| 11 | ESI & PF | Statutory payroll-linked, separate statute |

### Two ways to run it

**Option A — one command (recommended).** `run.py --tds` does everything
end-to-end: classify ledgers, apply LLM blocklist, scan vouchers, apply group
exclusion, write final list and JSON slices.

```bash
python run.py --tds
```

**Option B — manual review at each step.** Useful if you want to inspect or
hand-edit `expense_filtered.json` between the LLM filter and the voucher scan.
Thanks to **auto-detect**, once `expense_filtered.json` is written into `data/`,
every downstream script picks it up automatically — no extra flags needed.

```bash
# 1. Produce raw expense list
python analyze/list_expense_ledgers.py \
  --xml data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml \
  --json > expense_raw.json

# 2. LLM filter — writes data/expense_filtered.json + data/expense_blocklist_report.json
python tds/apply_expense_blocklist.py --input expense_raw.json \
  --config config/expense_blocklist_categories.json \
  --output data/expense_filtered.json \
  --report data/expense_blocklist_report.json

# 3. (Optional) inspect or hand-edit data/expense_filtered.json

# 4. Run the rest of the pipeline — auto-detects the sidecar
python run.py
```

Each downstream script logs the source it's using on stderr so you can confirm:
```
[core.ledger_sets] Auto-detected filter sidecar: expense_filtered.json (next to ledgers XML).
[core.ledger_sets] Loaded 1652 filtered expense names from expense_filtered.json.
```

To **opt out of auto-detect** (e.g. one-off non-TDS run while a sidecar exists),
run `run.py` without `--tds` — it calls `analyze/final_list.py` which ignores the sidecar
by default unless TDS mode is active. Or pass `--no-filter` directly to any
individual script:

```bash
python analyze/final_list.py \
  --ledgers data/tally_ledgers_final.xml \
  --daybook data/daybook_01042024_to_31032025.xml \
  --groups-xml data/tally_groups_final.xml --no-filter
```

### Caveats

- **First run is non-deterministic-feeling.** The LLM cache makes subsequent
  runs byte-identical, but the very first pass over a fresh ledger set will
  produce *one* set of decisions. Review `data/expense_blocklist_report.json`
  after the first run to verify the decisions before trusting the filtered output.
- **Cache must be deleted to force a re-classification** of a ledger whose
  decision you disagree with. Edit `expense_blocklist_cache.json` to remove
  the entry, then re-run.
- **The blocklist runs before the voucher scan.** If a blocklisted ledger only
  ever appears in vouchers that don't match the "expense Yes + liability No"
  pattern, removing it has no observable effect on the final output. The cost
  is borne in the LLM call regardless.
- **`expense_blocklist_categories.json` is the source of truth.** If your CA's
  guidance changes, edit this file and delete the cache to re-classify.
- **Cost.** A fresh run on ~1500 ledgers costs roughly $3–5 in Claude API
  usage (model: `claude-opus-4-7`). After the cache fills, re-runs are free.

---

## Data File Reference

| File | Produced by | Root element | Contents |
|---|---|---|---|
| `tally_groups_final.xml` | `export/tally_groups.py` | `<TALLYGROUPS>` | Group tree with `NATURE`, `ROOTPRIMARY`, `FINANCIALSTATEMENT` |
| `tally_ledgers_final.xml` | `export/tally_ledger_master.py` | `<TALLYLEDGERS>` | Enriched ledger master (GST, mailing, bank, tax, classification) |
| `daybook_DDMMYYYY_to_DDMMYYYY.xml` | `export/tally_daybook.py` | `<TALLYDAYBOOK>` | Deduplicated voucher register for the requested date range |
| `vouchers_by_final_list/*.xml` | `output/split_by_ledger.py` | `<TALLYDAYBOOK>` | Per-ledger slice — all vouchers referencing that ledger |
| `vouchers_by_final_list_json/*.json` | `output/to_json.py` | JSON object | `ledger_master` + `daybook` keys |
| `expense_blocklist_categories.json` | (committed) | JSON array | The 11 TDS blocklist categories — intent + keywords |
| `expense_filtered.json` | `tds/apply_expense_blocklist.py` / `tds/tds_expense_wrapper.py` | JSON array | Expense set with blocklisted names removed |
| `expense_blocklist_report.json` | `tds/apply_expense_blocklist.py` / `tds/tds_expense_wrapper.py` | JSON array | Per-name audit: blocklisted, category, reason, source |
| `expense_blocklist_cache.json` | `tds/apply_expense_blocklist.py` / `tds/tds_expense_wrapper.py` | JSON object | Persistent decision cache (key = lowercased name) |

---

## Output Folder Structure

```
TALLY_EXPORT/
│
├── export/                                     ← Phase 1: live Tally connection scripts
│   ├── __init__.py
│   ├── tally_groups.py
│   ├── tally_ledger_master.py
│   └── tally_daybook.py
│
├── core/                                       ← Shared utilities imported by multiple modules
│   ├── __init__.py
│   ├── groups.py                               ← BFS group-tree closure + ledger parent filter
│   └── ledger_sets.py                          ← load_expense_and_liability_sets() + TDS auto-detect
│
├── analyze/                                    ← Phase 2: offline ledger classification & pattern detection
│   ├── __init__.py
│   ├── list_liability_ledgers.py               ← list NATURE=Liability / ROOTPRIMARY=Current Assets
│   ├── list_expense_ledgers.py                 ← list NATURE=Expense / ROOTPRIMARY=Fixed Assets
│   ├── detect_cross_vouchers.py                ← cross-voucher pattern detection algorithm
│   └── final_list.py                           ← pattern ledgers minus group-excluded ledgers
│
├── output/                                     ← Phase 3: file generation (XML slices + JSON)
│   ├── __init__.py
│   ├── split_by_ledger.py                      ← one XML per target ledger
│   └── to_json.py                              ← XML slices → JSON with ledger master metadata
│
├── tds/                                        ← Phase 4 (optional): LLM-based TDS analysis
│   ├── __init__.py
│   ├── apply_expense_blocklist.py
│   └── tds_expense_wrapper.py
│
├── config/                                     ← Static config inputs (committed)
│   ├── expense_blocklist_categories.json       ← TDS mode: 11 PDF categories
│   └── .env.example                            ← template for ANTHROPIC_API_KEY
│
├── data/                                       ← ALL generated outputs (gitignored)
│   ├── tally_groups_final.xml                  ← generated by export/tally_groups.py
│   ├── tally_ledgers_final.xml                 ← generated by export/tally_ledger_master.py
│   ├── daybook_DDMMYYYY_to_DDMMYYYY.xml        ← generated by export/tally_daybook.py
│   ├── final.txt                               ← generated by analyze/final_list.py
│   ├── expense_filtered.json                   ← TDS mode: blocklist-filtered expense set
│   ├── expense_blocklist_report.json           ← TDS mode: per-name audit report
│   ├── expense_blocklist_cache.json            ← TDS mode: persistent LLM decision cache
│   ├── vouchers_by_final_list/                 ← generated by output/split_by_ledger.py
│   │   ├── Creditor A.xml
│   │   ├── Creditor B.xml
│   │   └── ...  (one file per target ledger)
│   └── vouchers_by_final_list_json/            ← generated by output/to_json.py
│       ├── Creditor A.json
│       ├── Creditor B.json
│       └── ...  (one file per target ledger)
│
├── run.py                                      ← single entry point for the full pipeline
├── requirements.txt                            ← runtime deps (requests + anthropic)
└── README.md
```

---

## Key Design Patterns

### 1. XML Sanitisation (`clean_tally_xml`)

Tally's XML responses frequently contain characters that are illegal in standard XML. A shared `clean_tally_xml()` function handles:

| Problem | Fix |
|---|---|
| Illegal decimal/hex char references (`&#0;`–`&#8;`) | Removed |
| Unescaped ampersands (`&`) | Replaced with `&amp;` when not already escaped |
| Raw control characters (0x00–0x1F) | Stripped |
| Namespace prefixes and `xmlns` declarations | Removed |

This sanitiser runs on the raw HTTP response string before any XML parsing.

---

### 2. Streaming Parse (`iterparse` + `elem.clear()`)

Daybook files can be hundreds of megabytes. All scripts that scan the daybook or ledger master use `xml.etree.ElementTree.iterparse` and call `elem.clear()` after processing each element to release memory immediately — the entire document is never held in RAM at once.

---

### 3. Ledger Deduplication & Merge

`tally_ledger_master.py` normalises ledger names (collapses whitespace) and detects duplicates. When two ledger records share the same normalised name:

- **Scalar fields:** Non-empty value preferred; if both non-empty and different, values are joined with `" | "`.
- **List fields (`*.LIST`):** Child rows are unioned; duplicate subtrees are skipped.

---

### 4. Voucher Pattern Matching

`analyze/detect_cross_vouchers.py` classifies each `<ENTRY>` line by cross-referencing the line's `LEDGERNAME` against two pre-built ledger sets (loaded from `core/ledger_sets.py`), then checks `ISDEEMEDPOSITIVE`:

```
ISDEEMEDPOSITIVE = "Yes"  →  debit-like  (increases asset / decreases liability)
ISDEEMEDPOSITIVE = "No"   →  credit-like (decreases asset / increases liability)
```

A voucher **matches** only if it contains **both**:
- An entry with `ISDEEMEDPOSITIVE == "Yes"` on an **expense or fixed-asset** ledger, **and**
- An entry with `ISDEEMEDPOSITIVE == "No"` on a **liability or current-asset** ledger.

The output collects the liability/current-asset names from condition 2, unioned across all matching vouchers.

---

### 5. Field Resolution Cascade

Tally stores the same business identifier (GSTIN, PAN, state) in multiple XML paths for historical reasons. `output/to_json.py` checks each path in priority order, uses the first non-empty value as canonical, and records the winning path in `field_sources` — so you can trace exactly where each value came from.

---

### 6. Filename Sanitisation + Collision Handling

Ledger names often contain characters that are illegal in filenames (`< > : " / \ | ? *`). The splitter replaces all such characters with `_`. If two distinct ledger names produce the same sanitised filename, subsequent files are suffixed `_2`, `_3`, etc.

---

### 7. Package Import Pattern

Every script that imports across package boundaries adds one line at the top to ensure the project root is on `sys.path`:

```python
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
```

Then uses absolute package imports:

```python
from core.groups import parent_names_from_roots, ledgers_with_parent_in, DEFAULT_ROOT_GROUPS
from core.ledger_sets import load_expense_and_liability_sets
from analyze.detect_cross_vouchers import collect_matching_liability_names
from analyze.final_list import load_final_ledger_names
```

This means scripts can be run both directly (`python analyze/final_list.py`) and imported as modules, without any `PYTHONPATH` setup required.

---

## Classification System

Both `tally_groups.py` and `tally_ledger_master.py` use the same hard-coded mapping from Tally's built-in primary groups to enrichment fields:

| Tally Primary Group | NATURE | FINANCIALSTATEMENT | ROOTPRIMARY |
|---|---|---|---|
| Capital Account | Liability | Balance Sheet | Capital Account |
| Reserves & Surplus | Liability | Balance Sheet | Reserves & Surplus |
| Loans (Liability) | Liability | Balance Sheet | Loans (Liability) |
| Current Liabilities | Liability | Balance Sheet | Current Liabilities |
| Duties & Taxes | Liability | Balance Sheet | Duties & Taxes |
| Provisions | Liability | Balance Sheet | Provisions |
| Suspense A/c | Liability | Balance Sheet | Suspense A/c |
| Branch / Divisions | Liability | Balance Sheet | Branch / Divisions |
| Fixed Assets | Asset | Balance Sheet | Fixed Assets |
| Investments | Asset | Balance Sheet | Investments |
| Current Assets | Asset | Balance Sheet | Current Assets |
| Loans & Advances (Asset) | Asset | Balance Sheet | Loans & Advances (Asset) |
| Misc. Expenses (ASSET) | Asset | Balance Sheet | Misc. Expenses (ASSET) |
| Stock-in-Hand | Asset | Balance Sheet | Stock-in-Hand |
| Direct Income | Income | P&L | Direct Income |
| Indirect Income | Income | P&L | Indirect Income |
| Sales Accounts | Income | P&L | Sales Accounts |
| Direct Expenses | Expense | P&L | Direct Expenses |
| Indirect Expenses | Expense | P&L | Indirect Expenses |
| Purchase Accounts | Expense | P&L | Purchase Accounts |

> **Consistency note:** If you modify this mapping in one script, apply the same change in the other to keep `NATURE` / `ROOTPRIMARY` consistent between group and ledger exports.

---

## JSON Output Structure

Each file in `vouchers_by_final_list_json/` follows this shape:

```json
{
  "ledger_master": {
    "NAME": "ABC Traders",
    "PARENT": "Sundry Creditors",
    "NATURE": "Liability",
    "ROOTPRIMARY": "Current Liabilities",
    "FINANCIALSTATEMENT": "Balance Sheet",
    "GSTIN": "27ABCDE1234F1Z5",
    "PAN": "ABCDE1234F",
    "STATE": "Maharashtra",
    "PINCODE": "400001",
    "MAILINGNAME": "ABC Traders Pvt Ltd",
    "COUNTRY": "India",
    "GSTIN_all_distinct": ["27ABCDE1234F1Z5"],
    "PAN_all_distinct": ["ABCDE1234F"],
    "field_sources": {
      "GSTIN": "PARTYGSTIN",
      "PAN": "INCOMETAXNUMBER",
      "STATE": "LEDMAILINGDETAILS.STATENAME",
      "PINCODE": "PINCODE"
    }
  },
  "daybook": {
    "@FROMDATE": "01-04-2024",
    "@TODATE": "31-03-2025",
    "@TOTALCOUNT": "42",
    "VOUCHER": [
      {
        "@DATE": "20240415",
        "@VOUCHERNUMBER": "PUR/001",
        "@VOUCHERTYPE": "Purchase",
        "PARTYLEDGERNAME": "ABC Traders",
        "LEDGERENTRIES": {
          "ENTRY": [
            {
              "LEDGERNAME": "Purchase Accounts",
              "ISDEEMEDPOSITIVE": "Yes",
              "AMOUNT": "-50000.00"
            },
            {
              "LEDGERNAME": "ABC Traders",
              "ISDEEMEDPOSITIVE": "No",
              "AMOUNT": "50000.00"
            }
          ]
        }
      }
    ]
  }
}
```

**Key points:**
- XML attributes are prefixed with `@` (e.g. `@DATE`, `@VOUCHERTYPE`).
- Repeated `<VOUCHER>` elements become a JSON array.
- `field_sources` maps each resolved `ledger_master` field to the XML tag path that supplied the value.
- `_lookup_error` appears in `ledger_master` when the filename stem has no matching `<LEDGER NAME="...">` in the master file (e.g. after a ledger rename or stale split).

---

## What You Get (Example Scale)

The table below shows **typical** output sizes from one full financial year export. Your actual numbers will vary by company size and voucher volume.

| Artifact | Typical size | Notes |
|---|---|---|
| `tally_groups_final.xml` | 50 – 100 KB | Tally typically ships with 130 – 160 groups |
| `tally_ledgers_final.xml` | 5 – 20 MB | Scales with number of ledgers in the company |
| `daybook_*.xml` | 50 – 250 MB | Scales with total voucher count for the date range |
| `vouchers_by_final_list/` | Hundreds of XML files | One per target ledger after voucher-pattern filtering |
| `vouchers_by_final_list_json/` | Matching JSON files | Mirror structure of the XML folder |

**Why so many files?** The splitter creates one XML/JSON per ledger in the **final list** (voucher-pattern ledgers minus excluded-group ledgers). For a company with hundreds of trade creditors or receivables this produces hundreds of files — one per counterparty — ready for downstream import or audit.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `requests.exceptions.ReadTimeout` during export | Tally timed out on a large response | Reduce the date range (smaller `--start` / `--end` window); consider quarterly chunks |
| Empty or malformed XML from Tally | HTTP server not enabled, or wrong port | In Tally go to **F12 → Advanced Configuration** and confirm the port (default 9000) |
| `xml.etree.ElementTree.ParseError` | Illegal characters survived `clean_tally_xml` | Inspect the raw response and extend the sanitiser regex |
| Ledger fields missing from output (no GSTIN etc.) | `FETCH` spec has fields unsupported by your Tally build | Comment out unknown field names in `tally_ledger_master.py`'s FETCH list |
| `_lookup_error` in JSON output | Ledger was renamed in Tally after the split was run | Re-run `output/split_by_ledger.py` then `output/to_json.py` |
| Fewer output files than expected | Pattern filter found no matching vouchers for some ledgers | Verify the daybook date range covers the relevant transaction period |
| Filename collisions (`_2`, `_3` suffix files) | Two ledger names sanitise to the same string | Expected and handled automatically; both files are written correctly |
| Very slow daybook export | Large company with many vouchers per month | Normal — each month is a separate HTTP round-trip with a 900 s read timeout |

---

## Notes & Caveats

- **Single entry point:** `run.py` is the recommended way to run the pipeline. It handles file paths, phase ordering, and cross-module imports automatically. Individual scripts can still be run directly — they insert the project root into `sys.path` at startup so no `PYTHONPATH` setup is needed.
- **Hardcoded endpoint:** All export scripts POST to `http://localhost:9000`. If Tally runs on a different port or remote host, update the URL constant in each export script.
- **No incremental sync:** Every export is a full pull. There is no delta-sync or checkpoint mechanism — re-run from scratch for updated data.
- **Nature classification:** The single source of truth is `core/nature.py` (`PRIMARY_NATURE`, `get_root_primary`, `classify_nature`), shared by `export/tally_groups.py` and `export/tally_ledger_master.py` so the two exports can never diverge. It layers the reserved-name walk with Tally's own `IsRevenue`/`IsDeemedPositive` flags and a closing-balance credit veto, so custom-named primary groups (e.g. a group literally named "Sales") are classified from their declared nature instead of falling to "Primary"/Unknown. Ledgers it cannot confidently type are stamped `NATURE="Review"` (quarantined out of the expense set, surfaced by `core.ledger_sets`), never silently treated as expenses.
- **Tally version compatibility:** The `FETCH` spec in `tally_ledger_master.py` targets recent Tally Prime builds. Older Tally.ERP 9 versions may not recognise all field names — remove unknown fields from the spec if the API returns an error response.
- **Large date ranges:** Even with a 900 s read timeout, very large companies exporting a full year may still experience timeouts. Use `--start` / `--end` with quarterly ranges if needed.
- **Offline after export:** Once the three master XML files exist in `data/`, all analysis and slicing scripts work entirely offline — no live Tally connection required.

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

*Built for Indian accounting data workflows. Contributions and issue reports welcome.*
