from pathlib import Path
import json

import pandas as pd


COLUMNS = [
    "id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "market_cap_rank",
    "total_volume",
    "price_change_percentage_24h",
    "last_updated",
]


def transform_raw_json(raw_json_path: Path) -> pd.DataFrame:
    with raw_json_path.open("r", encoding="utf-8") as f:
        records = json.load(f)

    df = pd.DataFrame(records)

    # Keep a stable analytic schema for downstream consumers.
    existing_columns = [col for col in COLUMNS if col in df.columns]
    df = df[existing_columns].copy()

    return df
