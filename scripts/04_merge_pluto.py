"""
Merge the grouped violations data from 03_filter_violations.py with the PLUTO
latitude/longitude data from 02_download_pluto.py, then serialize the result
to a JSON schema suitable for use in a SvelteKit app.

The two datasets are joined on the BBL (Borough-Block-Lot) identifier, which
is a unique 10-digit code that NYC uses to identify every tax lot.

Inputs:  output/bronx_c_violations.csv
         output/pluto_raw.csv
Output:  output/bronx_buildings.json
"""

import json
from ast import literal_eval
from pathlib import Path

import pandas as pd

# Resolve input/output paths relative to this script file, not the working directory
script_dir = Path(__file__).parent
output_dir = script_dir / "output"


def bbl_to_str(series: pd.Series) -> pd.Series:
    """
    Convert a BBL column to a clean string, handling the common case where
    pandas reads it as float64 (e.g. 2012340001.0) due to null values in
    the column. Without this fix, the trailing .0 prevents the join from
    matching rows across the two datasets.
    """
    # Drop nulls before converting so we don't produce the string "nan"
    mask = series.notna()
    result = series.copy().astype(object)
    result[mask] = series[mask].astype(float).astype(int).astype(str).str.strip()
    return result


# ------------------------------------------------------------------
# Step 1 – Load the violations data produced by 03_filter_violations.py
# ------------------------------------------------------------------

print("Loading grouped violations data ...")

violations = pd.read_csv(output_dir / "bronx_c_violations.csv")
violations["bbl"] = bbl_to_str(violations["bbl"])

# Report how many buildings are missing a BBL (they won't get coordinates)
null_bbl = violations["bbl"].isna().sum()
if null_bbl:
    print(
        f"  Warning: {null_bbl:,} buildings have a null BBL and will not receive coordinates."
    )

print(f"  {len(violations):,} buildings loaded.")

# ------------------------------------------------------------------
# Step 2 – Load the PLUTO data produced by 02_download_pluto.py
# ------------------------------------------------------------------

print("Loading PLUTO data ...")

pluto = pd.read_csv(output_dir / "pluto_raw.csv")
pluto["bbl"] = bbl_to_str(pluto["bbl"])

# Drop any duplicate BBL rows in PLUTO so the merge stays one-to-one
pluto = pluto.drop_duplicates(subset="bbl")

print(f"  {len(pluto):,} PLUTO records loaded.")

# ------------------------------------------------------------------
# Step 3 – Merge latitude and longitude onto the violations data
# ------------------------------------------------------------------

print("Merging latitude and longitude ...")

# Left-join the violations data with PLUTO on the BBL column.
# A left join keeps every building even if PLUTO has no matching record.
merged = violations.merge(pluto, on="bbl", how="left")

# Rename the PLUTO latitude/longitude columns to shorter names
merged = merged.rename(columns={"latitude": "lat", "longitude": "lng"})

# Report the match rate so the user can assess data quality
with_coords = merged["lat"].notna().sum()
without_coords = len(merged) - with_coords
print(f"  {with_coords:,} of {len(merged):,} buildings matched a PLUTO record.")
if without_coords:
    print(
        f"  {without_coords:,} buildings have no coordinates and will have null lat/lng in the output."
    )

# ------------------------------------------------------------------
# Step 4 – Convert to the JSON format used by the SvelteKit app
# ------------------------------------------------------------------

print("Converting to JSON ...")

# Parse the descriptions and dates columns back from their string representation.
# pandas stores lists as strings in CSV; literal_eval safely reconstructs them.
merged["descriptions"] = merged["descriptions"].apply(literal_eval)
merged["dates"] = merged["dates"].apply(literal_eval)

records = []

for _, row in merged.iterrows():
    # Pair each violation description with its corresponding date
    violations_list = [
        {"description": desc, "date": date}
        for desc, date in zip(row["descriptions"], row["dates"])
    ]

    record = {
        # HPD building identifier
        "id": str(int(row["buildingid"])),
        # Full street address
        "address": row["address"],
        # Five-digit ZIP code
        "zip": str(row["zipcode"]),
        # Total number of open Class C violations
        "violationCount": int(row["violationCount"]),
        # Date of the most recently issued violation (YYYY-MM-DD)
        "latestDate": row["latestDate"],
        # Latitude from PLUTO (None if no BBL match)
        "lat": round(float(row["lat"]), 4) if pd.notna(row["lat"]) else None,
        # Longitude from PLUTO (None if no BBL match)
        "lng": round(float(row["lng"]), 4) if pd.notna(row["lng"]) else None,
        # Individual violation records with description and date
        "violations": violations_list,
    }

    records.append(record)

# ------------------------------------------------------------------
# Step 5 – Save the output
# ------------------------------------------------------------------

output_dir.mkdir(exist_ok=True)
output_path = output_dir / "bronx_buildings.json"

with open(output_path, "w") as f:
    json.dump(records, f, indent=2)

print(f"Saved {len(records):,} buildings to {output_path}")
print("Copy this file to src/lib/data/bronx-buildings.json in your Svelte project.")
