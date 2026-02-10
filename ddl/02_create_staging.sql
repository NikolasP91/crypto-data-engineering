DROP TABLE IF EXISTS stg_crypto_market_tidy;

CREATE TABLE stg_crypto_market_tidy (
    extracted_at_utc TIMESTAMP,
    coin_id TEXT,
    symbol TEXT,
    name TEXT,
    vs_currency TEXT,
    open_time TIMESTAMP,
    close_time TIMESTAMP,
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    close_price NUMERIC,
    volume NUMERIC,
    quote_asset_volume NUMERIC,
    number_of_trades BIGINT
);