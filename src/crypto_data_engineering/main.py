from crypto_data_engineering.config import COIN_LIMIT, VS_CURRENCY
from crypto_data_engineering.pipelines.ingest import ingest_market_data
from crypto_data_engineering.pipelines.transform import transform_raw_json
from crypto_data_engineering.pipelines.load import write_processed_csv


def run() -> None:
    raw_path = ingest_market_data(vs_currency=VS_CURRENCY, limit=COIN_LIMIT)
    transformed = transform_raw_json(raw_path)
    output_csv = write_processed_csv(transformed)

    print(f"Raw data written to: {raw_path}")
    print(f"Processed data written to: {output_csv}")
    print(f"Rows written: {len(transformed)}")


if __name__ == "__main__":
    run()
