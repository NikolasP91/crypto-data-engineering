"""Transformation functions for converting raw JSON into tidy tabular rows."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

TARGET_COLUMNS: list[str] = [
    "extracted_at_utc",
    "coin_id",
    "symbol",
    "name",
    "vs_currency",
    "open_time",
    "close_time",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "quote_asset_volume",
    "number_of_trades",
]


def _read_raw_document(path: Path) -> dict:
    """Read and parse a raw JSON document from disk."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def transform_raw_files(raw_files: list[Path]) -> pd.DataFrame:
    """Transform raw hourly OHLCV JSON snapshots into a normalized dataframe.
    
    Each raw file contains 24 hourly candles for one coin.
    The output contains one row per hourly candle.
    """
    normalized_frames: list[pd.DataFrame] = []

    for path in raw_files:
        document = _read_raw_document(path)

        # market_data is now a list of hourly candles instead of a single dict
        hourly_candles = document.get("market_data", [])
        if not hourly_candles:
            continue

        # Convert list of candles into a dataframe
        frame = pd.DataFrame(hourly_candles)
        if frame.empty:
            continue

        # Add extraction metadata to each row
        frame["extracted_at_utc"] = document.get("extracted_at_utc")
        frame["vs_currency"] = document.get("vs_currency")
        normalized_frames.append(frame)

    if not normalized_frames:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    df = pd.concat(normalized_frames, ignore_index=True)

    # Normalize timestamp fields to readable ISO 8601 UTC strings
    # open_time and close_time are milliseconds since epoch from Binance
    if "open_time" in df.columns:
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if "close_time" in df.columns:
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Convert extraction timestamp (e.g. 20260210T125721Z) to ISO 8601
    if "extracted_at_utc" in df.columns:
        try:
            df["extracted_at_utc"] = pd.to_datetime(df["extracted_at_utc"], format="%Y%m%dT%H%M%SZ", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            # If parsing fails, leave original values
            pass

    # Keep only the target schema in a consistent column order
    df = df.reindex(columns=TARGET_COLUMNS)
    return df
