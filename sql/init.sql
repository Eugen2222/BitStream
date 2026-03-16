CREATE TABLE IF NOT EXISTS market_trade_clean (
    trade_id BIGINT,
    symbol TEXT,
    price NUMERIC(20, 8),
    quantity NUMERIC(20, 8),
    event_time_ms BIGINT,
    trade_time_ms BIGINT,
    event_time TIMESTAMP,
    trade_time TIMESTAMP,
    processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS btc_price_5s (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    symbol TEXT,
    avg_price NUMERIC(20,8),
    total_volume NUMERIC(20,8),
    trade_count BIGINT,
    processed_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS btc_kline_5s (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    symbol TEXT,
    open_price NUMERIC(20,8),
    high_price NUMERIC(20,8),
    low_price NUMERIC(20,8),
    close_price NUMERIC(20,8),
    total_volume NUMERIC(20,8),
    trade_count BIGINT,
    processed_at TIMESTAMP
);