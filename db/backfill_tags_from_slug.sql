-- Backfill tags_json and is_sports for markets where Gamma gave no tags but slug prefix is in polygon_category_mapping.
-- tag_prefix is the full value (e.g. sports,esports or sports,nhl); set tags_json = tag_prefix.
-- Usage: psql -h HOST -U USER -d tradeflux -f db/backfill_tags_from_slug.sql

INSERT INTO polygon_category_mapping (category_key, is_sports, tag_prefix) VALUES
  ('nhl', true, 'sports,nhl'), ('nfl', true, 'sports,nfl'), ('mlb', true, 'sports,mlb'), ('nba', true, 'sports,nba'),
  ('btc', false, 'crypto,btc'), ('eth', false, 'crypto,eth'), ('sol', false, 'crypto,sol'), ('xrp', false, 'crypto,xrp')
ON CONFLICT (category_key) DO NOTHING;

UPDATE polygon_markets m
SET tags_json = c.tag_prefix,
    is_sports = c.is_sports
FROM polygon_category_mapping c
WHERE c.category_key = lower(split_part(m.slug, '-', 1))
  AND c.tag_prefix IS NOT NULL
  AND m.slug IS NOT NULL
  AND (m.tags_json IS NULL OR trim(m.tags_json) = '');
