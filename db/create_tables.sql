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
CREATE INDEX IF NOT EXISTS idx_polygon_trades_block_timestamp ON polygon_trades(block_timestamp DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_polygon_trades_maker_asset ON polygon_trades(maker_asset_id);
CREATE INDEX IF NOT EXISTS idx_polygon_trades_taker_asset ON polygon_trades(taker_asset_id);
CREATE INDEX IF NOT EXISTS idx_polygon_trades_created_at ON polygon_trades(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_polygon_trades_transaction_hash ON polygon_trades(transaction_hash);

-- Token IDs observed on chain (from maker_asset_id / taker_asset_id). Use as “markets” seen on Polygon.
CREATE TABLE IF NOT EXISTS polygon_chain_tokens (
  token_id     TEXT PRIMARY KEY,
  first_block  BIGINT NOT NULL,
  last_block   BIGINT NOT NULL,
  updated_at   TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_polygon_chain_tokens_last_block ON polygon_chain_tokens(last_block);
CREATE INDEX IF NOT EXISTS idx_polygon_chain_tokens_first_block ON polygon_chain_tokens(first_block);

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

CREATE INDEX IF NOT EXISTS idx_polygon_market_details_resolved ON polygon_market_details(resolved_on_timestamp) WHERE resolved_on_timestamp IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_polygon_market_details_category ON polygon_market_details(category_group) WHERE category_group IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_polygon_market_details_league ON polygon_market_details(league_name) WHERE league_name IS NOT NULL;

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
CREATE INDEX IF NOT EXISTS idx_polygon_markets_yes_token ON polygon_markets(yes_token_id) WHERE yes_token_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_polygon_markets_no_token ON polygon_markets(no_token_id) WHERE no_token_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_polygon_markets_resolved_at ON polygon_markets(resolved_at) WHERE resolved_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS polygon_market_tokens (
  token_id     TEXT PRIMARY KEY,
  condition_id TEXT NOT NULL,
  outcome      TEXT NOT NULL CHECK (outcome IN ('YES', 'NO'))
);
CREATE INDEX IF NOT EXISTS idx_polygon_market_tokens_condition ON polygon_market_tokens(condition_id);

-- Category/series/slug keys that count as sports (incl. esports). Lookup key is lowercase.
-- Insert rows for Gamma category_group, events[].seriesSlug, or slug prefix (e.g. 'cs2', 'counter-strike', 'lol').
CREATE TABLE IF NOT EXISTS polymarket_category_mapping (
  category_key TEXT PRIMARY KEY,
  is_sports    BOOLEAN NOT NULL DEFAULT FALSE
);
COMMENT ON TABLE polymarket_category_mapping IS 'Maps Gamma category/series/slug prefix to is_sports; used when tags are missing (e.g. esports).';

-- Seed common sports/esports keys (idempotent: use ON CONFLICT DO NOTHING or run once).
INSERT INTO polymarket_category_mapping (category_key, is_sports) VALUES
  ('sports', true), ('counter-strike', true), ('cs2', true), ('csgo', true),
  ('league of legends', true), ('lol', true), ('dota 2', true), ('dota2', true), ('dota', true),
  ('valorant', true), ('val', true), ('overwatch', true), ('ow', true),
  ('rainbow six', true), ('r6', true), ('cod', true), ('rocket league', true),
  ('counter strike', true), ('esl pro league', true), ('madden', true), ('nba 2k', true),
  ('fighting', true), ('smash', true), ('tekken', true), ('street fighter', true), ('apex', true)
ON CONFLICT (category_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS sync_state (
  job_name             TEXT PRIMARY KEY,
  last_processed_block BIGINT,
  last_success_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE polygon_trades ADD COLUMN IF NOT EXISTS token_id TEXT;
ALTER TABLE polygon_trades ADD COLUMN IF NOT EXISTS price NUMERIC(40, 18);
ALTER TABLE polygon_trades ADD COLUMN IF NOT EXISTS shares NUMERIC(40, 18);
ALTER TABLE polygon_trades ADD COLUMN IF NOT EXISTS side TEXT;

-- Optional: speeds up ILIKE '%tag%' on polygon_market_details.tags (e.g. sport volume queries).
-- Requires: CREATE EXTENSION IF NOT EXISTS pg_trgm; (RDS allows it in public or custom schema)
-- CREATE INDEX IF NOT EXISTS idx_polygon_market_details_tags_gin ON polygon_market_details USING gin(tags gin_trgm_ops);

COMMENT ON TABLE polygon_trades IS 'OrderFilled events from Polygon Exchange contract (subgraph + WebSocket).';
COMMENT ON TABLE polygon_chain_tokens IS 'Token IDs seen on chain; represents markets with on-chain activity.';
COMMENT ON TABLE polygon_markets IS 'Canonical market metadata from Gamma API; ingest first, then trades.';
COMMENT ON TABLE polygon_market_tokens IS 'token_id -> condition_id + outcome (YES/NO) for mapping fills to markets.';
COMMENT ON TABLE sync_state IS 'Per-job cursor for restart-safe ingestion (e.g. polygon_trades subgraph timestamp).';
