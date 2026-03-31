"""
Clean the violation description field in the final JSON output.

Every HPD violation description begins with the same boilerplate legal
citation, followed by location-specific text describing the exact surface
and room where the lead-paint hazard was found.  This script:

  1. Splits the boilerplate from the location-specific text at the
     work-practices reference that ends every citation
     (``28 RCNY §11-06(B)(2)``).
  2. Keeps the boilerplate in the existing ``description`` field.
  3. Stores the location-specific text in a new ``specificDescription``
     field, converted from ALL-CAPS to sentence case for readability.

Input/Output:  output/bronx_buildings.json  (updated in place)
"""

import json
import re
from pathlib import Path

# Resolve paths relative to the repository root
script_dir = Path(__file__).parent.parent
output_dir = script_dir / "output"

# Pattern that matches the work-practices reference ending the boilerplate.
# The citation appears in several forms due to mixed casing and occasional
# encoding corruption (``§`` rendered as ``?``), so we match all variants.
_SPLIT_PATTERN = re.compile(r"[§?]11-06\([Bb]\)\(2\)")


def split_description(text: str) -> tuple[str, str]:
    """
    Split a violation description into its boilerplate legal citation and the
    location-specific detail that follows the work-practices reference.

    Returns a ``(boilerplate, specific)`` tuple.  If the marker is absent the
    whole text is returned as the boilerplate and ``specific`` is an empty
    string.
    """
    match = _SPLIT_PATTERN.search(text)
    if not match:
        return text.strip(), ""
    split_idx = match.end()
    boilerplate = text[:split_idx].strip()
    specific = text[split_idx:].strip()
    return boilerplate, specific


def to_sentence_case(text: str) -> str:
    """
    Convert an ALL-CAPS string to sentence case (first character capitalised,
    remainder lower-cased).

    Ordinal suffixes such as ``1st``, ``2nd``, ``3rd``, ``4th`` that are
    already lower-cased in the source data are preserved by this approach.
    """
    if not text:
        return text
    lowered = text.lower()
    return lowered[0].upper() + lowered[1:]


# ------------------------------------------------------------------
# Load
# ------------------------------------------------------------------

input_path = output_dir / "bronx_buildings.json"
print(f"Loading {input_path} ...")

with open(input_path) as f:
    buildings = json.load(f)

print(f"  {len(buildings):,} buildings loaded.")

# ------------------------------------------------------------------
# Clean descriptions
# ------------------------------------------------------------------

print("Cleaning violation descriptions ...")

cleaned = 0
unmatched = 0

for building in buildings:
    for violation in building.get("violations", []):
        raw = violation.get("description") or ""
        boilerplate, specific = split_description(raw)
        violation["description"] = boilerplate
        violation["specificDescription"] = to_sentence_case(specific)
        if specific:
            cleaned += 1
        else:
            unmatched += 1

print(f"  {cleaned:,} descriptions split and cleaned.")
if unmatched:
    print(
        f"  Warning: {unmatched:,} descriptions did not contain the expected split marker."
    )

# ------------------------------------------------------------------
# Save
# ------------------------------------------------------------------

with open(input_path, "w") as f:
    json.dump(buildings, f, indent=2)

print(f"Saved cleaned data to {input_path}")
