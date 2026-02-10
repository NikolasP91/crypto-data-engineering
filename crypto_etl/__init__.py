"""Crypto ETL package for CoinGecko market data ingestion and processing."""

from .pipeline import PipelineSummary, run_pipeline

__all__ = ["PipelineSummary", "run_pipeline"]
