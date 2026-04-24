# TALLY_EXPORT

Simple Python scripts for Tally data export and processing.

## What this project does
- Exports Tally groups, ledgers, and vouchers.
- Processes XML data into filtered outputs.
- Generates per-ledger JSON files.

## Main scripts
- `tally_groups.py`
- `tally_ledger_master.py`
- `tally_daybook.py`
- `split_daybook_by_final_list.py`
- `vouchers_to_json_with_ledger.py`

## Requirements
- Python 3.10+
- Tally running with XML API enabled (for export scripts)

## Run
```bash
python3 tally_groups.py
python3 tally_ledger_master.py
python3 tally_daybook.py
```

## Notes
- Large data files and generated folders are ignored via `.gitignore`.
