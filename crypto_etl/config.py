"""Central configuration values for the crypto ETL project."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Base directories
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
DATA_DIR: Path = PROJECT_ROOT / "data"
RAW_DIR: Path = DATA_DIR / "raw"
PROCESSED_DIR: Path = DATA_DIR / "processed"

# Binance API settings
BINANCE_BASE_URL: str = "https://api.binance.com/api/v3"
BINANCE_API_KEY: str = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET: str = os.getenv("BINANCE_API_SECRET", "")
BINANCE_KLINES_ENDPOINT: str = "/klines"
BINANCE_KLINES_INTERVAL: str = "1h"  # Hourly candlestick data
BINANCE_KLINES_LIMIT: int = 24  # Last 24 hours of hourly data

# Extraction settings
REQUEST_TIMEOUT_SECONDS: int = 30
COIN_IDS: list[str] = ["bitcoin"]
BINANCE_SYMBOLS: dict[str, str] = {
    "bitcoin": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "solana": "SOLUSDT",
    "cardano": "ADAUSDT",
    "dogecoin": "DOGEUSDT",
}
VS_CURRENCY: str = "usd"

# Binance API rate limiting (much higher limits with API key)
REQUEST_DELAY_SECONDS: float = 0.5  # Binance allows 1200 requests per minute with API key
MAX_RETRIES: int = 3
INITIAL_BACKOFF_SECONDS: float = 1.0
