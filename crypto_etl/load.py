"""Loading functions for persisting transformed data assets."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _utc_timestamp() -> str:
    """Return a filesystem-safe UTC timestamp string."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_processed_csv(df: pd.DataFrame, output_dir: Path) -> Path:
    """Write transformed data to a timestamped CSV and return the output path."""
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = _utc_timestamp()
    output_path = output_dir / f"crypto_market_tidy_{timestamp}.csv"
    df.to_csv(output_path, index=False)

    return output_path
