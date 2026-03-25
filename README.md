# nyc-hpd-violations

A Python pipeline that downloads and processes NYC Housing Maintenance Code Violations from [NYC Open Data](https://data.cityofnewyork.us/Housing-Development/Housing-Maintenance-Code-Violations/wvxf-dwi5), producing a clean JSON file with one record per building that groups violations by address, counts them, and adds geographic coordinates.

Originally developed as a data source for a journalism class exercise in building interactive database explorers.

## What it produces

Running the four scripts in order produces `output/bronx_buildings.json` — every building in the Bronx with at least one open Class C (immediately hazardous) HPD violation, with:

- Violation count and most recent date
- Street address and ZIP code
- Latitude and longitude from [MapPLUTO](https://data.cityofnewyork.us/City-Government/Primary-Land-Use-Tax-Lot-Output-PLUTO-/64uk-42ks)
- A nested list of all individual violations with descriptions and dates

Intermediate outputs are stored as compressed Parquet files to reduce disk usage and speed reloads.

By editing two constants at the top of `01_download_violations.py` you can filter for any borough or violation class instead.

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
# Step 1 — download open Bronx Class C violations → output/violations_raw.parquet
uv run python scripts/01_download_violations.py

# Step 2 — download PLUTO coordinates → output/pluto_raw.parquet
uv run python scripts/02_download_pluto.py

# Step 3 — group violations by building → output/bronx_c_violations.parquet
uv run python scripts/03_filter_violations.py

# Step 4 — merge coordinates and write JSON → output/bronx_buildings.json
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
