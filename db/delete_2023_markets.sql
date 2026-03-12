-- Delete markets whose slug contains 2023 (e.g. event date 2023-xx-xx) and related rows.
-- Run: psql -h HOST -U USER -d tradeflux -f db/delete_2023_markets.sql

DELETE FROM polygon_market_tokens WHERE condition_id IN (SELECT condition_id FROM polygon_markets WHERE slug LIKE '%2023%');
DELETE FROM polygon_token_condition WHERE condition_id IN (SELECT condition_id FROM polygon_markets WHERE slug LIKE '%2023%');
DELETE FROM polygon_market_details WHERE condition_id IN (SELECT condition_id FROM polygon_markets WHERE slug LIKE '%2023%');
DELETE FROM polygon_markets WHERE slug LIKE '%2023%';
