import json
from pathlib import Path

from crypto_data_engineering.pipelines.transform import transform_raw_json


def test_transform_raw_json_selects_expected_columns(tmp_path: Path) -> None:
    payload = [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 50000,
            "market_cap": 1000000000,
            "market_cap_rank": 1,
            "total_volume": 25000000,
            "price_change_percentage_24h": 2.3,
            "last_updated": "2026-01-01T00:00:00Z",
            "extra_field": "ignored",
        }
    ]

    raw_file = tmp_path / "raw.json"
    raw_file.write_text(json.dumps(payload), encoding="utf-8")

    df = transform_raw_json(raw_file)

    assert list(df.columns) == [
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
    assert len(df) == 1
    assert df.iloc[0]["id"] == "bitcoin"
