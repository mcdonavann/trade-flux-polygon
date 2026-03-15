-- Compare to Dune: NBA resolved trade count + notional for same date window.
-- Logic: resolved markets only, EXACT tag 'nba' (explode tags, trim, lower = 'nba').
--
-- Run:
--   psql ... -v start_ts='2026-03-14 00:00:00' -v end_ts='2026-03-15 00:00:00' -f db/nba_volume_compare_dune.sql
--
-- On Dune run (same window + resolved + exact nba) to compare:
--
--   SELECT
--     COUNT(*) AS trade_count,
--     SUM(t.shares)/2 AS notional_raw,
--     (SUM(t.shares)/2)/1e6 AS notional_millions
--   FROM polymarket_polygon.market_trades t
--   JOIN polymarket_polygon.market_details m
--     ON CAST(t.condition_id AS varchar) = CAST(m.condition_id AS varchar)
--   WHERE t.block_time >= TIMESTAMP '2026-03-14'
--     AND t.block_time < TIMESTAMP '2026-03-15'
--     AND m.resolved_on_timestamp IS NOT NULL
--     AND EXISTS (
--       SELECT 1 FROM unnest(split(m.tags, ',')) u(tag)
--       WHERE TRIM(LOWER(tag)) = 'nba'
--     );

WITH date_window AS (
  SELECT :'start_ts'::timestamptz AS start_window, :'end_ts'::timestamptz AS end_window
),
trades_by_market AS (
  SELECT
    c.condition_id,
    SUM(COALESCE(t.shares, t.taker_amount)) AS sum_shares
  FROM polygon_trades t
  JOIN polygon_token_condition c ON c.token_id = t.maker_asset_id::text OR c.token_id = t.taker_asset_id::text
  CROSS JOIN date_window w
  WHERE t.block_timestamp >= w.start_window AND t.block_timestamp < w.end_window
  GROUP BY c.condition_id
),
traded_markets AS (SELECT DISTINCT condition_id FROM trades_by_market),
resolved_markets AS (
  SELECT DISTINCT condition_id FROM polygon_market_details WHERE resolved_on_timestamp IS NOT NULL
),
nba_markets AS (
  SELECT DISTINCT md.condition_id
  FROM polygon_market_details md
  JOIN traded_markets tm ON md.condition_id = tm.condition_id
  CROSS JOIN LATERAL unnest(
    COALESCE(string_to_array(NULLIF(TRIM(md.tags), ''), ','), ARRAY[]::text[])
  ) AS u(tag)
  WHERE md.tags IS NOT NULL AND TRIM(LOWER(TRIM(tag))) = 'nba'
),
-- Notional: same formula as nba_volume_dune_style.sql
notional_agg AS (
  SELECT (SUM(t.sum_shares) / 2) / 1e6 AS notional_millions
  FROM trades_by_market t
  JOIN nba_markets nm ON t.condition_id = nm.condition_id
  WHERE EXISTS (SELECT 1 FROM resolved_markets rm WHERE rm.condition_id = t.condition_id)
),
-- Trade count: one row per trade (condition_id from maker or taker)
trade_rows AS (
  SELECT DISTINCT t.order_hash
  FROM polygon_trades t
  JOIN polygon_token_condition c ON c.token_id = t.maker_asset_id::text OR c.token_id = t.taker_asset_id::text
  CROSS JOIN date_window w
  WHERE t.block_timestamp >= w.start_window AND t.block_timestamp < w.end_window
    AND EXISTS (SELECT 1 FROM resolved_markets r WHERE r.condition_id = c.condition_id)
    AND EXISTS (SELECT 1 FROM nba_markets n WHERE n.condition_id = c.condition_id)
)
SELECT
  (SELECT COUNT(DISTINCT order_hash) FROM trade_rows) AS trade_count,
  (SELECT notional_millions FROM notional_agg) AS notional_millions;
