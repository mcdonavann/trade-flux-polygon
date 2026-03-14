-- Clear all trades and markets so app can start fresh from POLYGON_SUBGRAPH_MIN_TIMESTAMP (2026-03-13 07:40 UTC).
-- Run once: psql -h HOST -U USER -d tradeflux -f db/clear_trades_and_markets.sql

-- 1. Trades and subgraph cursor
TRUNCATE TABLE polygon_trades;
DELETE FROM sync_state WHERE job_name = 'polygon_trades';

-- 2. Markets (order respects logical dependencies)
TRUNCATE TABLE polygon_market_tokens;
TRUNCATE TABLE polygon_token_condition;
TRUNCATE TABLE polygon_market_details;
TRUNCATE TABLE polygon_markets;

-- Optional: clear chain tokens so market sync re-fetches from trades
-- TRUNCATE TABLE polygon_chain_tokens;
