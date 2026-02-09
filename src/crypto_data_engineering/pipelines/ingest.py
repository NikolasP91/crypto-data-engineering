import json
from datetime import datetime, timezone
from pathlib import Path

from crypto_data_engineering.clients.coingecko import fetch_markets


RAW_DIR = Path("data/raw")


def ingest_market_data(vs_currency: str, limit: int) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    payload = fetch_markets(vs_currency=vs_currency, per_page=limit)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = RAW_DIR / f"coingecko_markets_{timestamp}.json"

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    return output_path
