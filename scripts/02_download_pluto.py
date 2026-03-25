"""
Download the latitude, longitude, and BBL columns from the NYC MapPLUTO
dataset, which will later be merged with the violations data to add
geographic coordinates to each building.

The dataset is fetched in pages of 50,000 rows so no single request
risks timing out.

Output: output/pluto_raw.parquet

Source: NYC Open Data – Primary Land Use Tax Lot Output (MapPLUTO)
https://data.cityofnewyork.us/City-Government/Primary-Land-Use-Tax-Lot-Output-PLUTO-/64uk-42ks
"""

from pathlib import Path

import pandas as pd

# Set the base URL for the NYC Open Data Socrata API endpoint for PLUTO
url = "https://data.cityofnewyork.us/resource/64uk-42ks.csv"

# Set the number of rows to request per page; 50,000 is a safe size for the Socrata API
page_size = 50_000

# Print a status message so the user knows the download is about to begin
print("Downloading PLUTO from NYC Open Data (paginating) ...")

# Collect each page of results in this list; we will concatenate them afterwards
pages = []

# Start the offset counter at zero; it increases by page_size after each successful page
offset = 0

# Keep fetching pages until a page comes back with fewer rows than page_size (signals last page)
while True:
    # Request only the three columns we need; smaller responses are faster and less likely to time out
    page_url = (
        f"{url}?$select=bbl,latitude,longitude&$limit={page_size}&$offset={offset}"
    )

    # Download this page of results into a DataFrame
    page = pd.read_csv(page_url)

    # Add this page to the collection
    pages.append(page)

    # Report progress so the user can see the download moving forward
    print(f"  Fetched {offset + len(page):,} rows so far ...")

    # If the page has fewer rows than the page size, we have reached the last page
    if len(page) < page_size:
        break

    # Advance the offset by one full page to request the next batch
    offset += page_size

# Stack all pages into a single DataFrame
pluto = pd.concat(pages, ignore_index=True)

# Print the final row count so the user can confirm the full dataset arrived
print(f"Downloaded {len(pluto):,} PLUTO records.")

# Resolve output path relative to this script file, not the working directory
output_dir = Path(__file__).parent / "output"
output_dir.mkdir(exist_ok=True)

# Save the raw PLUTO data to a Parquet file for use by the merge script
output_path = output_dir / "pluto_raw.parquet"
pluto.to_parquet(output_path, index=False, compression="snappy")

# Tell the user where the file was saved
print(f"Saved {len(pluto):,} rows to {output_path}")
