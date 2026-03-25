"""
Download open Class C housing violations for the Bronx from the NYC HPD
Housing Maintenance Code Violations dataset.

Filtering is done server-side via Socrata SoQL query parameters so only the
rows we need are transferred. The matching rows are fetched in pages of
50,000 so no single request risks timing out.

Output: output/violations_raw.csv

Source: NYC Open Data – Housing Maintenance Code Violations
https://data.cityofnewyork.us/Housing-Development/Housing-Maintenance-Code-Violations/wvxf-dwi5

Configuration
-------------
Change the constants below to download a different borough or violation class.

BORO_ID values:
  1 = Manhattan
  2 = Bronx
  3 = Brooklyn
  4 = Queens
  5 = Staten Island

VIOLATION_CLASS values:
  'A' = non-hazardous
  'B' = hazardous
  'C' = immediately hazardous
"""

from pathlib import Path
from urllib.parse import quote

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration — edit these to filter for a different borough or class
# ---------------------------------------------------------------------------

# Borough to filter on (2 = Bronx)
BORO_ID = 2

# Violation class to filter on ('C' = immediately hazardous)
VIOLATION_CLASS = "C"

# Only include violations that have not been resolved
CURRENT_STATUS = "Open"

# ---------------------------------------------------------------------------

# Set the base URL for the NYC Open Data Socrata API endpoint for HPD violations
url = "https://data.cityofnewyork.us/resource/wvxf-dwi5.csv"

# Set the number of rows to request per page; 50,000 is a safe size for the Socrata API
page_size = 50_000

# Build the SoQL WHERE clause to pre-filter on the server.
# Note: class is a reserved word in SoQL on some versions of the API; if you
# receive a 400 error, try replacing "class" with "violationclass".
where = f"boroid={BORO_ID} AND class='{VIOLATION_CLASS}' AND currentstatus='{CURRENT_STATUS}'"

# URL-encode the WHERE clause so quotes and spaces survive the HTTP request
encoded_where = quote(where)

# Print a status message so the user knows the download is about to begin
print(
    f"Downloading open Bronx Class {VIOLATION_CLASS} violations from NYC Open Data (paginating) ..."
)

# Collect each page of results in this list; we will concatenate them afterwards
pages = []

# Start the offset counter at zero; it increases by page_size after each successful page
offset = 0

# Keep fetching pages until a page comes back with fewer rows than page_size (signals last page)
while True:
    # Build the full URL for this page using SoQL $where, $limit, and $offset parameters
    page_url = f"{url}?$where={encoded_where}&$limit={page_size}&$offset={offset}"

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
violations = pd.concat(pages, ignore_index=True)

# Print the final row count so the user can confirm the full dataset arrived
print(f"Downloaded {len(violations):,} open Bronx Class {VIOLATION_CLASS} violations.")

# Resolve output path relative to this script file, not the working directory
output_dir = Path(__file__).parent / "output"
output_dir.mkdir(exist_ok=True)

# Save the raw violation rows to a CSV file for use by the filter script
output_path = output_dir / "violations_raw.csv"
violations.to_csv(output_path, index=False)

# Tell the user where the file was saved
print(f"Saved {len(violations):,} rows to {output_path}")
