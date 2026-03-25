"""
Read the raw violations downloaded by 01_download_violations.py, group them
by building, and write a summary Parquet file with one row per building.

Each building row contains the violation count, the date of the most recent
violation, and a nested list of all violation descriptions and dates for use
in the final JSON output.

Input:  output/violations_raw.parquet
Output: output/bronx_c_violations.parquet
"""

from pathlib import Path

import pandas as pd

# Resolve input/output paths relative to this script file, not the working directory
script_dir = Path(__file__).parent
output_dir = script_dir / "output"

# Print a status message so the user knows the script has started
print("Loading raw violations data ...")

# Read the Parquet file that 01_download_violations.py wrote into a DataFrame
violations = pd.read_parquet(output_dir / "violations_raw.parquet")

# Print the row count so the user can confirm the file loaded completely
print(f"  {len(violations):,} violation rows loaded.")

# Report how many rows are missing a BBL before we do anything with it
null_bbl = violations["bbl"].isna().sum()
if null_bbl:
    print(
        f"  Warning: {null_bbl:,} rows have a null BBL and will not match PLUTO coordinates."
    )

# Build a full street address by combining the house number and street name columns
violations["address"] = (
    violations["housenumber"].str.strip() + " " + violations["streetname"].str.strip()
)

# Convert the violation issue date to a proper datetime type so max() returns the latest date correctly
violations["novissueddate"] = pd.to_datetime(violations["novissueddate"])

# Print a status message for the grouping step
print("Grouping violations by building ...")

# Group the individual violation rows by building, aggregating each group into one summary row
buildings = (
    violations
    # Group by the HPD building identifier plus fields that are the same for every row in a building
    .groupby(["buildingid", "address", "zipcode", "bbl"])
    # Count violations, find the most recent date, and collect all descriptions and dates
    .agg(
        # Count the number of open Class C violations for this building
        violationCount=("violationid", "count"),
        # Capture the date of the most recent violation at this building
        latestDate=("novissueddate", "max"),
        # Gather every violation description into a list for later use in the JSON output
        descriptions=("novdescription", list),
        # Gather every violation date into a parallel list for later use in the JSON output
        dates=("novissueddate", list),
    )
    # Drop the groupby index so buildingid becomes a plain column again
    .reset_index()
)

# Format latestDate as a plain YYYY-MM-DD string so it is readable in Parquet and JSON
buildings["latestDate"] = buildings["latestDate"].dt.strftime("%Y-%m-%d")

# Format each violation date in the lists as a plain string as well
buildings["dates"] = buildings["dates"].apply(
    lambda lst: [d.strftime("%Y-%m-%d") for d in lst]
)

# Sort the buildings by violation count in descending order so the worst buildings come first
buildings = buildings.sort_values("violationCount", ascending=False)

# Make the output directory if it does not already exist
output_dir.mkdir(exist_ok=True)

# Save the grouped building data to a Parquet file for use in the merge script
output_path = output_dir / "bronx_c_violations.parquet"
buildings.to_parquet(output_path, index=False, compression="snappy")

# Tell the user how many buildings were found and where the file was saved
print(f"Saved {len(buildings):,} buildings to {output_path}")
