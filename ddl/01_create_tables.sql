-- =========================
-- DIMENSION TABLES
-- =========================

CREATE TABLE IF NOT EXISTS dim_coin (
    coin_id SERIAL PRIMARY KEY,
    coin_code TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_currency (
    currency_id SERIAL PRIMARY KEY,
    currency_code TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_interval (
    interval_id SERIAL PRIMARY KEY,
    interval_name TEXT NOT NULL UNIQUE,
    interval_seconds INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id BIGINT PRIMARY KEY,
    ts_utc TIMESTAMP NOT NULL UNIQUE,
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

CREATE TABLE IF NOT EXISTS fact_candle (
    candle_id BIGSERIAL PRIMARY KEY,
    coin_id INTEGER REFERENCES dim_coin(coin_id),
    currency_id INTEGER REFERENCES dim_currency(currency_id),
    interval_id INTEGER REFERENCES dim_interval(interval_id),
    open_time_utc TIMESTAMP REFERENCES dim_time(ts_utc),
    close_time_utc TIMESTAMP REFERENCES dim_time(ts_utc),
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    close_price NUMERIC,
    volume NUMERIC,
    quote_asset_volume NUMERIC,
    number_of_trades INTEGER
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'fact_candle'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fact_candle_coin_id_currency_id_interval_id_open_time_utc_key'
    ) THEN
        DELETE FROM fact_candle a
        USING fact_candle b
        WHERE a.candle_id < b.candle_id
          AND a.coin_id = b.coin_id
          AND a.currency_id = b.currency_id
          AND a.interval_id = b.interval_id
          AND a.open_time_utc = b.open_time_utc;

        ALTER TABLE fact_candle
        ADD CONSTRAINT fact_candle_coin_id_currency_id_interval_id_open_time_utc_key
        UNIQUE (coin_id, currency_id, interval_id, open_time_utc);
    END IF;
END $$;
