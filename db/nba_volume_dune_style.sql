-- NBA notional volume matching Dune logic:
-- - Resolved markets only
-- - Date window (start_date to end_date)
-- - Tag: EXACT 'nba' (explode tags, trim, lower, match = 'nba')
-- - Volume: SUM(shares) / 2 then scale by 1e6 for display
--
-- Run (25-min window like Dune):
--   psql ... -v start_ts='2026-03-01 00:00:00' -v end_ts='2026-03-01 00:25:00' -f db/nba_volume_dune_style.sql

WITH date_window AS (
  SELECT
    :'start_ts'::timestamptz AS start_window,
    :'end_ts'::timestamptz AS end_window
),
trades_by_market AS (
  SELECT
    c.condition_id,
    SUM(COALESCE(t.shares, t.taker_amount)) AS sum_shares
  FROM polygon_trades t
  JOIN polygon_token_condition c ON c.token_id = t.maker_asset_id::text OR c.token_id = t.taker_asset_id::text
  CROSS JOIN date_window w
  WHERE t.block_timestamp >= w.start_window
    AND t.block_timestamp < w.end_window
  GROUP BY c.condition_id
),
traded_markets AS (
  SELECT DISTINCT condition_id FROM trades_by_market
),
resolved_markets AS (
  SELECT DISTINCT condition_id
  FROM polygon_market_details
  WHERE resolved_on_timestamp IS NOT NULL
),
-- Explode tags (comma-split, trim, lower) and keep only EXACT 'nba'
nba_markets AS (
  SELECT DISTINCT md.condition_id
  FROM polygon_market_details md
  JOIN traded_markets tm ON md.condition_id = tm.condition_id
  CROSS JOIN LATERAL unnest(
    COALESCE(string_to_array(NULLIF(TRIM(md.tags), ''), ','), ARRAY[]::text[])
  ) AS u(tag)
  WHERE md.tags IS NOT NULL
    AND TRIM(LOWER(TRIM(tag))) = 'nba'
)
SELECT (SUM(t.sum_shares) / 2) / 1e6 AS nba_notional_volume
FROM trades_by_market t
JOIN nba_markets nm ON t.condition_id = nm.condition_id
WHERE EXISTS (SELECT 1 FROM resolved_markets rm WHERE rm.condition_id = t.condition_id);
