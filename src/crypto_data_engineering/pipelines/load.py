from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


PROCESSED_DIR = Path("data/processed")


def write_processed_csv(df: pd.DataFrame) -> Path:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = PROCESSED_DIR / f"markets_{timestamp}.csv"
    df.to_csv(output_path, index=False)
    return output_path
