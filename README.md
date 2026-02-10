# Crypto Data Engineering ETL

A clean Python 3.11 ETL project for ingesting cryptocurrency market data from the CoinGecko API, transforming it into a tidy tabular schema, and saving curated CSV outputs.

## Project Structure

```text
crypto-data-engineering/
├── .gitignore
├── README.md
├── requirements.txt
├── run_etl.py
└── crypto_etl/
    ├── __init__.py
    ├── config.py
    ├── extract.py
    ├── transform.py
    ├── load.py
    └── pipeline.py
```

## Prerequisites

- Python 3.11+
- Internet access to query the CoinGecko API

## Install

1. Create and activate a virtual environment.
2. Install dependencies.

```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
```

## Configuration

Defaults are defined in `crypto_etl/config.py`:

- `VS_CURRENCY`: quote currency (default `usd`)
- `COIN_IDS`: list of CoinGecko coin IDs to fetch
- `RAW_DIR`: raw JSON output directory
- `PROCESSED_DIR`: processed CSV output directory

You can edit these constants directly to customize the pipeline.

## Run the Pipeline

From the project root:

```bash
python run_etl.py
```

Expected outputs:

- Raw JSON snapshots in `data/raw/`
- Processed CSV in `data/processed/`

All files are timestamped in UTC.

## Notes

- The pipeline creates required directories automatically.
- HTTP failures are handled with clear error messages.
- Transformation logic uses `pandas` for flattening and normalization.
