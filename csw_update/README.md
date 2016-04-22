# Installation
If you don't have Pip, [get Pip](https://pip.pypa.io/en/latest/installing.html)!

Either clone the repo or [download as a zip](https://github.com/CIC-Geospatial-Data-Discovery-Project/metadata-resources/archive/master.zip). Once cloned/downloaded and extracted, run this from the root of the repo:

    pip install -r requirements.txt

# Configuration
1. Make a copy of `config.py.sample`
2. Name it `config.py`
3. Fill in `CSW_URL`, `USER`, and `PASSWORD`

# Export from GeoNetwork
Export a csv for the records you want to edit via spreadsheet. We use a custom csv export for the CIC: GDDP (mainly for link handling), but as long as you have the `uuid` field, the tool should work with the out of the box CSV export.

# Spreadsheet Input Structural Requirements
- The input file format **must be** `csv`. Excel formats (`xls` or `xlsx`) are not supported.
- There **must be** a column called "uuid". The values in it should correspond to the fileIdentifier for the records being updated. For example: `bdfc5cd2-732d-4559-a9c7-df38dd683aec`.
- See [FIELDS.md](FIELDS.md) for fields that can be manipulated via CSV, and what the columns must be named in order to be recognized.
