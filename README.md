# nyc-hpd-violations

A Python pipeline that downloads and processes NYC Housing Maintenance Code Violations from [NYC Open Data](https://data.cityofnewyork.us/Housing-Development/Housing-Maintenance-Code-Violations/wvxf-dwi5), focused on **lead paint violations** (Class C, `ordernumber` 616/617). It produces a clean JSON file with one record per building that groups violations by address, counts them, and adds geographic coordinates.

Originally developed as a data source for a journalism class exercise in building interactive database explorers.

## What it produces

Running the four scripts in order produces `scripts/output/bronx_buildings.json` — every Bronx building with at least one open lead paint (Class C, ordernumber 616/617) HPD violation, with:

- Violation count and most recent date
- Street address and ZIP code
- Latitude and longitude from [MapPLUTO](https://data.cityofnewyork.us/City-Government/Primary-Land-Use-Tax-Lot-Output-PLUTO-/64uk-42ks)
- A nested list of all individual violations with descriptions, dates, and current status (sorted most recent first)

Additional filters applied during download:

- Violation class `C`
- `ordernumber` in 616 or 617 (lead paint violations per HPD)

Intermediate outputs are stored as compressed Parquet files to reduce disk usage and speed reloads.

By editing two constants at the top of `01_download_violations.py` you can filter for any borough or violation class instead.

### What counts as “open”

The download keeps only HPD violations whose `currentstatus` is one of these “open” states (per the HPD data dictionary), excluding closed/dismissed cases:

- VIOLATION OPEN — Active, still requires action or pending reinspection window
- NOV SENT OUT — Notice of Violation mailed to the owner
- NOV CERTIFIED ON TIME — Certified corrected on time and accepted
- NOV CERTIFIED LATE — Certified corrected but submitted late; not acceptable
- CERTIFICATION POSTPONMENT GRANTED — Extension to correct/certify granted
- CERTIFICATION POSTPONMENT DENIED — Extension request denied; original date stands
- FALSE CERTIFICATION — Certified corrected but reinspection found issues
- VIOLATION WILL BE REINSPECTED — Reinspection scheduled
- DEFECT LETTER ISSUED — Correction observed but documentation pending
- NOT COMPLIED WITH — Reinspection found condition persists
- FIRST NO ACCESS TO RE- INSPECT VIOLATION — No access on first reinspection attempt
- SECOND NO ACCESS TO RE-INSPECT VIOLATION — No access on second reinspection attempt
- VIOLATION REOPEN — Reopened after being closed
- INFO NOV SENT OUT — Informational notice about an order (no cert date)
- INVALID CERTIFICATION — Certification submitted but defective
- COMPLIED IN ACCESS AREA — Partial compliance observed (window guard context)
- CIV 14 MAILED — Tenant notified of received owner certification

## Requirements

Python 3.11 or later and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

## Running the pipeline

```bash
make
```

That runs all four steps in order. Or run them individually:

```bash
# Step 1 — download open Bronx Class C violations → scripts/output/violations_raw.parquet
uv run python scripts/01_download_violations.py

# Step 2 — download PLUTO coordinates → scripts/output/pluto_raw.parquet
uv run python scripts/02_download_pluto.py

# Step 3 — group violations by building → scripts/output/bronx_c_violations.parquet
uv run python scripts/03_filter_violations.py

# Step 4 — merge coordinates and write JSON → scripts/output/bronx_buildings.json
uv run python scripts/04_merge_pluto.py
```

Steps 1 and 2 only need to run once. You can re-run steps 3 and 4 as many times as you like without re-downloading the raw data, which is useful if you're adjusting the output schema.

The `make` command uses file-based dependency tracking, so it will only re-run steps whose inputs have changed.

## Filtering for a different borough or class

Open `scripts/01_download_violations.py` and edit the constants near the top:

```python
# Borough to filter on (2 = Bronx)
BORO_ID = 2

# Violation class to filter on ('C' = immediately hazardous)
VIOLATION_CLASS = "C"
```

Borough IDs: 1 = Manhattan, 2 = Bronx, 3 = Brooklyn, 4 = Queens, 5 = Staten Island.

Violation classes: A = non-hazardous, B = hazardous, C = immediately hazardous.

## Data sources

| Dataset                                    | Source                                                                                                           |
| ------------------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| HPD Housing Maintenance Code Violations    | [NYC Open Data](https://data.cityofnewyork.us/Housing-Development/Housing-Maintenance-Code-Violations/wvxf-dwi5) |
| Primary Land Use Tax Lot Output (MapPLUTO) | [NYC Open Data](https://data.cityofnewyork.us/City-Government/Primary-Land-Use-Tax-Lot-Output-PLUTO-/64uk-42ks)  |

## License

[MIT](LICENSE)
