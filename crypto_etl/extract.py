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
) -> list[dict]:
    """Fetch hourly OHLCV candlestick data for the last 24 hours from Binance.
    
    Uses the /api/v3/klines endpoint which provides:
    - Open, High, Low, Close, Volume for each hour
    
    Returns a list of hourly candles for the last 24 hours.
    Implements exponential backoff retry logic for handling rate limit errors (429).
    """
    url = f"{config.BINANCE_BASE_URL}{config.BINANCE_KLINES_ENDPOINT}"
    params = {
        "symbol": symbol,
        "interval": config.BINANCE_KLINES_INTERVAL,
        "limit": config.BINANCE_KLINES_LIMIT,
    }
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

    # Parse Binance klines response
    # Each kline is: [open_time, open, high, low, close, volume, close_time, quote_asset_volume, trades, taker_buy_base, taker_buy_quote, ignore]
    klines = response.json()
    
    if not klines:
        raise RuntimeError(f"Binance returned no klines for {coin_id} ({symbol}).")
    
    # Transform each kline into a normalized market data object
    hourly_data = []
    for kline in klines:
        candle = {
            "coin_id": coin_id,
            "symbol": symbol.replace("USDT", "").lower(),
            "name": coin_id.capitalize(),
            "open_time": kline[0],
            "open_price": float(kline[1]),
            "high_price": float(kline[2]),
            "low_price": float(kline[3]),
            "close_price": float(kline[4]),
            "volume": float(kline[5]),
            "close_time": kline[6],
            "quote_asset_volume": float(kline[7]),
            "number_of_trades": int(kline[8]),
        }
        hourly_data.append(candle)
    
    return hourly_data


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
    """Extract hourly market data for configured coins from Binance and persist raw JSON files.
    
    Each raw file contains 24 hourly OHLCV candles for one coin.
    """
    timestamp = _utc_timestamp()
    written_files: list[Path] = []

    print("Starting extraction of hourly OHLCV data from Binance API...")

    with requests.Session() as session:
        for i, coin_id in enumerate(coin_ids):
            # Add delay between requests to respect rate limits
            if i > 0:
                time.sleep(config.REQUEST_DELAY_SECONDS)
            
            symbol = config.BINANCE_SYMBOLS.get(coin_id, f"{coin_id.upper()}USDT")
            print(f"Fetching hourly data for {coin_id} ({symbol})...")
            
            hourly_candles = fetch_market_data_for_coin(
                coin_id=coin_id,
                symbol=symbol,
                session=session,
            )
            output_path = write_raw_json(
                coin_id=coin_id,
                market_data=hourly_candles,
                extracted_at_utc=timestamp,
                output_dir=output_dir,
                vs_currency=vs_currency,
            )
            written_files.append(output_path)

    return written_files
