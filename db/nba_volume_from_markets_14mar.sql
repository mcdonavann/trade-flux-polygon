-- Sum of polygon_markets.volume for NBA resolved markets (using market table).
-- NBA = exact tag 'nba' from polygon_market_details; resolved = polygon_markets.is_resolved.
--
-- Run: psql ... -f db/nba_volume_from_markets_14mar.sql

WITH nba_markets AS (
  SELECT DISTINCT md.condition_id
  FROM polygon_market_details md
  CROSS JOIN LATERAL unnest(
    COALESCE(string_to_array(NULLIF(TRIM(md.tags), ''), ','), ARRAY[]::text[])
  ) AS u(tag)
  WHERE md.tags IS NOT NULL
    AND TRIM(LOWER(TRIM(tag))) = 'nba'
)
SELECT
  COUNT(*) AS nba_resolved_markets_with_volume,
  SUM(m.volume) AS nba_volume_sum
FROM polygon_markets m
JOIN nba_markets nm ON m.condition_id = nm.condition_id
WHERE m.is_resolved = true
  AND m.volume IS NOT NULL;
