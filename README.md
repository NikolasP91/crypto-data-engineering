# Crypto Data Engineering ETL

Python ETL project that extracts hourly OHLCV candles from the Binance API, transforms them into a tidy CSV, and can load them into a PostgreSQL data warehouse (staging + dimensions + fact).

## Project Structure

```text
crypto-data-engineering/
|-- README.md
|-- requirements.txt
|-- run_etl.py
|-- crypto_etl/
|   |-- config.py
|   |-- extract.py
|   |-- transform.py
|   |-- load.py
|   `-- pipeline.py
|-- ddl/
|   |-- 01_create_tables.sql
|   |-- 02_create_staging.sql
|   `-- 03_populate_dw.sql
`-- scripts/
    |-- run_pipeline_to_dw.ps1
    `-- convert_csv_timestamps.py
```

## Prerequisites

- Python 3.11+
- Internet access (Binance API)
- PostgreSQL server (for DW load)
- `psql` client executable (in `PATH` or configured via `PSQL_PATH`)

## Install

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Configuration

The ETL reads `.env` (loaded by `crypto_etl/config.py`), and the DW script also loads `.env` automatically.

Create a `.env` in project root:

```dotenv
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret

PSQL_PATH=D:\postgres_data\bin\psql.exe
PGHOST=localhost
PGPORT=5432
PGDATABASE=postgres
PGUSER=postgres
PGPASSWORD=your_password
```

Notes:
- `BINANCE_API_KEY` and `BINANCE_API_SECRET` are optional for basic klines calls, but supported.
- `PSQL_PATH` is useful when `psql.exe` is not in system `PATH`.

## Run ETL Only

```powershell
python run_etl.py
```

Outputs:
- Raw JSON snapshots in `data/raw/`
- Processed CSV in `data/processed/` (`crypto_market_tidy_<UTC timestamp>.csv`)

## Run ETL + Data Warehouse Load

From project root:

```powershell
. .\scripts\run_pipeline_to_dw.ps1
```

What this script does:
- Runs ETL (`run_etl.py`)
- Creates DW tables (`ddl/01_create_tables.sql`)
- Recreates staging (`ddl/02_create_staging.sql`)
- Bulk loads latest processed CSV into staging via `\copy`
- Populates dimensions and fact table (`ddl/03_populate_dw.sql`)

## Warehouse Schema (High Level)

- Dimensions: `dim_coin`, `dim_currency`, `dim_interval`, `dim_time`
- Fact: `fact_candle`
- Staging: `stg_crypto_market_tidy`

## Troubleshooting

- `psql was not found`:
  - Set `PSQL_PATH` in `.env` to full `psql.exe` path.
- `password authentication failed`:
  - Verify `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` in `.env`.
- Script not found when running `. .\scripts\run_pipeline_to_dw.ps1`:
  - `cd` into project root first, or use absolute path to the script.
