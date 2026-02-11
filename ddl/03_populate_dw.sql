BEGIN;

INSERT INTO dim_coin (coin_code, symbol, name)
SELECT DISTINCT
    s.coin_id AS coin_code,
    s.symbol,
    s.name
FROM stg_crypto_market_tidy s
WHERE s.coin_id IS NOT NULL
ON CONFLICT (coin_code) DO UPDATE
SET
    symbol = EXCLUDED.symbol,
    name = EXCLUDED.name;

INSERT INTO dim_currency (currency_code)
SELECT DISTINCT s.vs_currency
FROM stg_crypto_market_tidy s
WHERE s.vs_currency IS NOT NULL
ON CONFLICT (currency_code) DO NOTHING;

INSERT INTO dim_interval (interval_name, interval_seconds)
VALUES ('1h', 3600)
ON CONFLICT (interval_name) DO NOTHING;

WITH distinct_timestamps AS (
    SELECT DISTINCT s.open_time AS ts_utc
    FROM stg_crypto_market_tidy s
    WHERE s.open_time IS NOT NULL
    UNION
    SELECT DISTINCT s.close_time AS ts_utc
    FROM stg_crypto_market_tidy s
    WHERE s.close_time IS NOT NULL
)
INSERT INTO dim_time (
    time_id,
    ts_utc,
    date,
    year,
    month,
    day,
    hour,
    day_of_week,
    week_of_year
)
SELECT
    EXTRACT(EPOCH FROM dt.ts_utc)::BIGINT AS time_id,
    dt.ts_utc,
    dt.ts_utc::DATE AS date,
    EXTRACT(YEAR FROM dt.ts_utc)::INTEGER AS year,
    EXTRACT(MONTH FROM dt.ts_utc)::INTEGER AS month,
    EXTRACT(DAY FROM dt.ts_utc)::INTEGER AS day,
    EXTRACT(HOUR FROM dt.ts_utc)::INTEGER AS hour,
    EXTRACT(DOW FROM dt.ts_utc)::INTEGER AS day_of_week,
    EXTRACT(WEEK FROM dt.ts_utc)::INTEGER AS week_of_year
FROM distinct_timestamps dt
ON CONFLICT (time_id) DO NOTHING;

INSERT INTO fact_candle (
    coin_id,
    currency_id,
    interval_id,
    open_time_id,
    close_time_id,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    quote_asset_volume,
    number_of_trades
)
SELECT
    c.coin_id,
    cur.currency_id,
    i.interval_id,
    ot.time_id AS open_time_id,
    ct.time_id AS close_time_id,
    s.open_price,
    s.high_price,
    s.low_price,
    s.close_price,
    s.volume,
    s.quote_asset_volume,
    s.number_of_trades::INTEGER
FROM stg_crypto_market_tidy s
JOIN dim_coin c
    ON c.coin_code = s.coin_id
JOIN dim_currency cur
    ON cur.currency_code = s.vs_currency
JOIN dim_interval i
    ON i.interval_name = '1h'
JOIN dim_time ot
    ON ot.ts_utc = s.open_time
JOIN dim_time ct
    ON ct.ts_utc = s.close_time
ON CONFLICT (coin_id, currency_id, interval_id, open_time_id) DO UPDATE
SET
    close_time_id = EXCLUDED.close_time_id,
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    volume = EXCLUDED.volume,
    quote_asset_volume = EXCLUDED.quote_asset_volume,
    number_of_trades = EXCLUDED.number_of_trades;

COMMIT;
