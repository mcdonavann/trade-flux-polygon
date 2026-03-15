-- Sum of polygon_markets.volume for College Basketball resolved markets (using market table).
-- Tags: college-basketball, march-madness, ncaa-basketball, ncaa-cbb, ncaa-tournament, ncaa
--   (DB uses hyphens; also match ncaab, college basketball etc. for compatibility).
--
-- Run: psql ... -f db/cbb_volume_from_markets.sql

WITH cbb_tags AS (
  SELECT unnest(ARRAY[
    'ncaab', 'college basketball', 'ncaa basketball', 'march madness', 'ncaa tournament', 'big 10', 'big ten',
    'college-basketball', 'march-madness', 'ncaa-basketball', 'ncaa-cbb', 'ncaa-tournament', 'ncaa'
  ]) AS tag
),
cbb_markets AS (
  SELECT DISTINCT md.condition_id
  FROM polygon_market_details md
  CROSS JOIN LATERAL unnest(
    COALESCE(string_to_array(NULLIF(TRIM(md.tags), ''), ','), ARRAY[]::text[])
  ) AS u(tag)
  CROSS JOIN cbb_tags r
  WHERE md.tags IS NOT NULL
    AND TRIM(LOWER(TRIM(u.tag))) = r.tag
)
-- Resolved = polygon_market_details.resolved_on_timestamp
SELECT
  COUNT(*) FILTER (WHERE md.resolved_on_timestamp IS NOT NULL) AS cbb_resolved_with_volume,
  SUM(m.volume) FILTER (WHERE md.resolved_on_timestamp IS NOT NULL) AS cbb_volume_sum_resolved,
  COUNT(*) AS cbb_all_with_volume,
  SUM(m.volume) AS cbb_volume_sum_all
FROM polygon_markets m
JOIN cbb_markets c ON m.condition_id = c.condition_id
LEFT JOIN polygon_market_details md ON md.condition_id = m.condition_id
WHERE m.volume IS NOT NULL;
