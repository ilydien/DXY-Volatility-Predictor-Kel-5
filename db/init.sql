CREATE TABLE IF NOT EXISTS market_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    UNIQUE (timestamp, ticker)
);

CREATE INDEX IF NOT EXISTS idx_market_data_ts ON market_data (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_market_data_ticker_ts ON market_data (ticker, timestamp DESC);

CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    predicted_close DOUBLE PRECISION NOT NULL,
    actual_close DOUBLE PRECISION,
    features JSONB
);

CREATE INDEX IF NOT EXISTS idx_predictions_ts ON predictions (timestamp DESC);
