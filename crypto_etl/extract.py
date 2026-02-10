"""Extraction functions for pulling cryptocurrency market data from Binance."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from crypto_etl import config


def _utc_timestamp() -> str:
    """Return a filesystem-safe UTC timestamp string."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _get_binance_headers() -> dict[str, str]:
    """Get headers for Binance API requests (includes API key if configured)."""
    headers = {}
    if config.BINANCE_API_KEY:
        headers["X-MBX-APIKEY"] = config.BINANCE_API_KEY
    return headers


def fetch_market_data_for_coin(
    coin_id: str,
    symbol: str,
    session: requests.Session,
) -> dict:
    """Fetch 24h market metrics for one coin from Binance.
    
    Uses the /api/v3/ticker/24hr endpoint which provides:
    - Current price, 24h high/low, volume, price change
    
    Implements exponential backoff retry logic for handling rate limit errors (429).
    """
    url = f"{config.BINANCE_BASE_URL}/ticker/24hr"
    params = {"symbol": symbol}
    headers = _get_binance_headers()

    backoff_delay = config.INITIAL_BACKOFF_SECONDS
    
    for attempt in range(config.MAX_RETRIES + 1):
        try:
            response = session.get(
                url, 
                params=params, 
                headers=headers,
                timeout=config.REQUEST_TIMEOUT_SECONDS
            )
            response.raise_for_status()
            break  # Success, exit retry loop
        except requests.HTTPError as exc:
            if response.status_code == 429 and attempt < config.MAX_RETRIES:
                # Rate limited; wait and retry with exponential backoff
                print(f"Rate limited (429) for '{coin_id}' ({symbol}). Retrying in {backoff_delay:.1f}s...")
                time.sleep(backoff_delay)
                backoff_delay *= 2  # Exponential backoff
            else:
                # Not a 429 or out of retries
                raise RuntimeError(f"Failed to fetch data for '{coin_id}': {exc}") from exc
        except requests.RequestException as exc:
            raise RuntimeError(f"Failed to fetch data for '{coin_id}': {exc}") from exc

    # Parse and normalize Binance response
    binance_data = response.json()
    
    # Extract relevant fields and normalize to our schema
    market_data = {
        "id": coin_id,
        "symbol": symbol.replace("USDT", "").lower(),
        "name": coin_id.capitalize(),
        "current_price": float(binance_data.get("lastPrice", 0)),
        "high_24h": float(binance_data.get("highPrice", 0)),
        "low_24h": float(binance_data.get("lowPrice", 0)),
        "price_change_24h": float(binance_data.get("priceChange", 0)),
        "price_change_percentage_24h": float(binance_data.get("priceChangePercent", 0)),
        "total_volume": float(binance_data.get("volume", 0)),
        # Fields not available from Binance spot ticker
        "market_cap": None,
        "market_cap_rank": None,
        "circulating_supply": None,
        "total_supply": None,
        "max_supply": None,
        "last_updated": binance_data.get("time"),
    }
    
    return market_data


def write_raw_json(
    coin_id: str,
    market_data: dict,
    extracted_at_utc: str,
    output_dir: Path,
    vs_currency: str,
) -> Path:
    """Write one raw JSON snapshot for a coin and return the file path."""
    output_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"{coin_id}_{extracted_at_utc}.json"
    output_path = output_dir / file_name

    raw_document = {
        "extracted_at_utc": extracted_at_utc,
        "coin_id": coin_id,
        "vs_currency": vs_currency,
        "source": "binance",
        "market_data": market_data,
    }

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(raw_document, f, indent=2)

    return output_path


def extract_market_data(
    coin_ids: list[str],
    vs_currency: str,
    output_dir: Path,
) -> list[Path]:
    """Extract market data for configured coins from Binance and persist raw JSON files."""
    timestamp = _utc_timestamp()
    written_files: list[Path] = []

    print("Starting extraction from Binance API...")

    with requests.Session() as session:
        for i, coin_id in enumerate(coin_ids):
            # Add delay between requests to respect rate limits
            if i > 0:
                time.sleep(config.REQUEST_DELAY_SECONDS)
            
            symbol = config.BINANCE_SYMBOLS.get(coin_id, f"{coin_id.upper()}USDT")
            print(f"Fetching {coin_id} ({symbol})...")
            
            market_data = fetch_market_data_for_coin(
                coin_id=coin_id,
                symbol=symbol,
                session=session,
            )
            output_path = write_raw_json(
                coin_id=coin_id,
                market_data=market_data,
                extracted_at_utc=timestamp,
                output_dir=output_dir,
                vs_currency=vs_currency,
            )
            written_files.append(output_path)

    return written_files
