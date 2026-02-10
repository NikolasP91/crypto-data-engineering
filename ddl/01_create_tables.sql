-- =========================
-- DIMENSION TABLES
-- =========================

CREATE TABLE dim_coin (
    coin_id SERIAL PRIMARY KEY,
    coin_code TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE dim_currency (
    currency_id SERIAL PRIMARY KEY,
    currency_code TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_interval (
    interval_id SERIAL PRIMARY KEY,
    interval_name TEXT NOT NULL UNIQUE,
    interval_seconds INTEGER NOT NULL
);

CREATE TABLE dim_time (
    time_id BIGINT PRIMARY KEY,
    ts_utc TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL
);

-- =========================
-- FACT TABLE
-- =========================

CREATE TABLE fact_candle (
    candle_id BIGSERIAL PRIMARY KEY,
    coin_id INTEGER REFERENCES dim_coin(coin_id),
    currency_id INTEGER REFERENCES dim_currency(currency_id),
    interval_id INTEGER REFERENCES dim_interval(interval_id),
    open_time_id BIGINT REFERENCES dim_time(time_id),
    close_time_id BIGINT REFERENCES dim_time(time_id),
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    close_price NUMERIC,
    volume NUMERIC,
    quote_asset_volume NUMERIC,
    number_of_trades INTEGER,
    UNIQUE (coin_id, currency_id, interval_id, open_time_id)
);
