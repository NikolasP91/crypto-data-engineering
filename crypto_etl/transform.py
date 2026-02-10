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
    "current_price",
    "market_cap",
    "market_cap_rank",
    "total_volume",
    "high_24h",
    "low_24h",
    "price_change_24h",
    "price_change_percentage_24h",
    "circulating_supply",
    "total_supply",
    "max_supply",
    "last_updated",
]


def _read_raw_document(path: Path) -> dict:
    """Read and parse a raw JSON document from disk."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def transform_raw_files(raw_files: list[Path]) -> pd.DataFrame:
    """Transform raw JSON snapshots into a normalized dataframe.
    
    Handles market data from any source (CoinGecko, Binance, etc.) and normalizes
    to a consistent schema. Missing fields are filled with NaN.
    """
    normalized_frames: list[pd.DataFrame] = []

    for path in raw_files:
        document = _read_raw_document(path)

        # Flatten nested API payload keys into top-level columns.
        frame = pd.json_normalize(document.get("market_data", {}))
        if frame.empty:
            continue

        # Preserve extraction metadata alongside normalized market metrics.
        frame["extracted_at_utc"] = document.get("extracted_at_utc")
        frame["coin_id"] = document.get("coin_id")
        frame["vs_currency"] = document.get("vs_currency")
        normalized_frames.append(frame)

    if not normalized_frames:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    df = pd.concat(normalized_frames, ignore_index=True)

    # Keep only the target schema in a consistent column order.
    df = df.reindex(columns=TARGET_COLUMNS)
    return df
