"""ETL pipeline orchestration for CoinGecko cryptocurrency market data."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from crypto_etl import config
from crypto_etl.extract import extract_market_data
from crypto_etl.load import write_processed_csv
from crypto_etl.transform import transform_raw_files


@dataclass(slots=True)
class PipelineSummary:
    """High-level metadata emitted after a successful pipeline run."""

    raw_files: list[Path]
    processed_file: Path
    row_count: int


def ensure_directories() -> None:
    """Ensure all required output directories exist before the pipeline runs."""
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def run_pipeline() -> PipelineSummary:
    """Execute extract, transform, and load stages end to end."""
    ensure_directories()

    raw_files = extract_market_data(
        coin_ids=config.COIN_IDS,
        vs_currency=config.VS_CURRENCY,
        output_dir=config.RAW_DIR,
    )

    transformed_df = transform_raw_files(raw_files)
    processed_file = write_processed_csv(transformed_df, config.PROCESSED_DIR)

    return PipelineSummary(
        raw_files=raw_files,
        processed_file=processed_file,
        row_count=len(transformed_df),
    )
