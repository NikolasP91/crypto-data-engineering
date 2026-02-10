"""Command-line entrypoint for running the crypto ETL pipeline."""

from __future__ import annotations

from crypto_etl.pipeline import run_pipeline


def main() -> None:
    """Run the ETL pipeline and print a concise execution summary."""
    summary = run_pipeline()

    print("Crypto ETL pipeline completed successfully.")
    print(f"Raw files written: {len(summary.raw_files)}")
    for path in summary.raw_files:
        print(f"  - {path}")

    print(f"Processed CSV: {summary.processed_file}")
    print(f"Rows loaded: {summary.row_count}")


if __name__ == "__main__":
    main()
