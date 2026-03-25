.PHONY: all install lint

# Run the full pipeline from scratch
all: output/bronx_buildings.json

# Clean up all generated files
clean:
	rm -f output/*.parquet output/*.json

# Install dependencies
install:
	uv sync
	uv run pre-commit install

# Lint the scripts
lint:
	uv run ruff check scripts/

# Step 1: Download raw HPD violations (Bronx, Class C, Open)
output/violations_raw.parquet:
	uv run python scripts/01_download_violations.py

# Step 2: Download PLUTO coordinates
output/pluto_raw.parquet:
	uv run python scripts/02_download_pluto.py

# Step 3: Group violations by building
output/bronx_c_violations.parquet: output/violations_raw.parquet
	uv run python scripts/03_filter_violations.py

# Step 4: Merge coordinates and write final JSON
output/bronx_buildings.json: output/bronx_c_violations.parquet output/pluto_raw.parquet
	uv run python scripts/04_merge_pluto.py
