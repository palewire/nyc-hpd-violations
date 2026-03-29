# nyc-hpd-bronx-lead-paint-violations

A Python pipeline that downloads and processes NYC Housing Maintenance Code Violations from [NYC Open Data](https://data.cityofnewyork.us/Housing-Development/Housing-Maintenance-Code-Violations/wvxf-dwi5), focused on lead paint violations in the Bronx.

It produces a clean JSON file with one record per building that groups violations by address, counts them, and adds geographic coordinates.

Originally developed as a data source for ["Coding the News,"](https://palewi.re/docs/coding-the-news/) a course at the CUNY Graduate School of Journalism.

## Requirements

Python 3.11 or later and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

## Running the pipeline

```bash
make
```
