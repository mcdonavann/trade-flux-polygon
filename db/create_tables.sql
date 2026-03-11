-- Polymarket on-chain data from Polygon (Exchange contract OrderFilled events).
-- Same DB as trade-flux-poly. Run once: psql -h HOST -U USER -d tradeflux -f db/create_tables.sql

-- Trades: one row per OrderFilled (deduplicated by order_hash).
CREATE TABLE IF NOT EXISTS polygon_trades (
  order_hash       TEXT PRIMARY KEY,
  block_number     BIGINT NOT NULL,
  block_timestamp  TIMESTAMPTZ,
  transaction_hash TEXT NOT NULL,
  log_index        INT NOT NULL,
  contract_address TEXT NOT NULL,
  maker            TEXT NOT NULL,
  taker            TEXT NOT NULL,
  maker_asset_id   NUMERIC(78, 0) NOT NULL,
  taker_asset_id   NUMERIC(78, 0) NOT NULL,
  maker_amount     NUMERIC(40, 0) NOT NULL,
  taker_amount     NUMERIC(40, 0) NOT NULL,
  fee              NUMERIC(40, 0),
  created_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (transaction_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_polygon_trades_block ON polygon_trades(block_number);
CREATE INDEX IF NOT EXISTS idx_polygon_trades_maker_asset ON polygon_trades(maker_asset_id);
CREATE INDEX IF NOT EXISTS idx_polygon_trades_taker_asset ON polygon_trades(taker_asset_id);

-- Token IDs observed on chain (from maker_asset_id / taker_asset_id). Use as “markets” seen on Polygon.
CREATE TABLE IF NOT EXISTS polygon_chain_tokens (
  token_id     TEXT PRIMARY KEY,
  first_block  BIGINT NOT NULL,
  last_block   BIGINT NOT NULL,
  updated_at   TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_polygon_chain_tokens_last_block ON polygon_chain_tokens(last_block);

-- condition_id links trade to market (backfilled from polygon_token_condition)
ALTER TABLE polygon_trades ADD COLUMN IF NOT EXISTS condition_id TEXT;
CREATE INDEX IF NOT EXISTS idx_polygon_trades_condition_id ON polygon_trades(condition_id) WHERE condition_id IS NOT NULL;

-- Market metadata from Polymarket Gamma API.
CREATE TABLE IF NOT EXISTS polygon_market_details (
  condition_id          TEXT PRIMARY KEY,
  slug                  TEXT,
  question              TEXT,
  tags                  TEXT,
  resolved_on_timestamp TIMESTAMPTZ,
  closed_time           TEXT,
  is_sports             BOOLEAN DEFAULT FALSE,
  sport_name            TEXT,
  league_name           TEXT,
  category_group        TEXT,
  updated_at            TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS polygon_token_condition (
  token_id     TEXT PRIMARY KEY,
  condition_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_polygon_token_condition_condition ON polygon_token_condition(condition_id);

CREATE TABLE IF NOT EXISTS polygon_markets (
  condition_id   TEXT PRIMARY KEY,
  slug           TEXT,
  question       TEXT,
  yes_token_id   TEXT,
  no_token_id    TEXT,
  created_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  updated_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  is_resolved    BOOLEAN DEFAULT FALSE,
  resolved_at    TIMESTAMPTZ,
  is_sports      BOOLEAN DEFAULT FALSE,
  sport_name     TEXT,
  category_group TEXT,
  tags_json      TEXT
);
CREATE INDEX IF NOT EXISTS idx_polygon_markets_resolved ON polygon_markets(is_resolved) WHERE is_resolved = true;
CREATE INDEX IF NOT EXISTS idx_polygon_markets_sports ON polygon_markets(is_sports) WHERE is_sports = true;

CREATE TABLE IF NOT EXISTS polygon_market_tokens (
  token_id     TEXT PRIMARY KEY,
  condition_id TEXT NOT NULL,
  outcome      TEXT NOT NULL CHECK (outcome IN ('YES', 'NO'))
);
CREATE INDEX IF NOT EXISTS idx_polygon_market_tokens_condition ON polygon_market_tokens(condition_id);

CREATE TABLE IF NOT EXISTS sync_state (
  job_name             TEXT PRIMARY KEY,
  last_processed_block BIGINT,
  last_success_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE polygon_trades ADD COLUMN IF NOT EXISTS token_id TEXT;
ALTER TABLE polygon_trades ADD COLUMN IF NOT EXISTS price NUMERIC(40, 18);
ALTER TABLE polygon_trades ADD COLUMN IF NOT EXISTS shares NUMERIC(40, 18);
ALTER TABLE polygon_trades ADD COLUMN IF NOT EXISTS side TEXT;

COMMENT ON TABLE polygon_trades IS 'OrderFilled events from Polygon Exchange contract (subgraph + WebSocket).';
COMMENT ON TABLE polygon_chain_tokens IS 'Token IDs seen on chain; represents markets with on-chain activity.';
COMMENT ON TABLE polygon_markets IS 'Canonical market metadata from Gamma API; ingest first, then trades.';
COMMENT ON TABLE polygon_market_tokens IS 'token_id -> condition_id + outcome (YES/NO) for mapping fills to markets.';
COMMENT ON TABLE sync_state IS 'Per-job cursor for restart-safe ingestion (e.g. polygon_trades subgraph timestamp).';
