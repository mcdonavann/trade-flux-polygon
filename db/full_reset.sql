-- Full reset: clear all polygon/market/trade data for fresh start.
-- Run with app stopped. Usage: psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f db/full_reset.sql

TRUNCATE TABLE polygon_trades CASCADE;
TRUNCATE TABLE polygon_market_tokens CASCADE;
TRUNCATE TABLE polygon_market_details CASCADE;
TRUNCATE TABLE polygon_markets CASCADE;
TRUNCATE TABLE polygon_token_condition CASCADE;
TRUNCATE TABLE polygon_chain_tokens CASCADE;
TRUNCATE TABLE polygon_ws_subscribed_tokens CASCADE;
TRUNCATE TABLE polygon_category_mapping CASCADE;

DELETE FROM sync_state WHERE job_name = 'polygon_trades';

-- Re-seed category mapping (sports/crypto tags)
INSERT INTO polygon_category_mapping (category_key, is_sports, tag_prefix) VALUES
  ('sports', true, 'sports'),
  ('counter-strike', true, 'sports,esports'), ('cs2', true, 'sports,esports'), ('csgo', true, 'sports,esports'),
  ('league of legends', true, 'sports,esports'), ('lol', true, 'sports,esports'), ('dota 2', true, 'sports,esports'), ('dota2', true, 'sports,esports'), ('dota', true, 'sports,esports'),
  ('valorant', true, 'sports,esports'), ('val', true, 'sports,esports'), ('overwatch', true, 'sports,esports'), ('ow', true, 'sports,esports'),
  ('rainbow six', true, 'sports,esports'), ('r6', true, 'sports,esports'), ('cod', true, 'sports,esports'), ('rocket league', true, 'sports,esports'),
  ('counter strike', true, 'sports,esports'), ('esl pro league', true, 'sports,esports'), ('madden', true, 'sports,esports'), ('nba 2k', true, 'sports,esports'),
  ('fighting', true, 'sports,esports'), ('smash', true, 'sports,esports'), ('tekken', true, 'sports,esports'), ('street fighter', true, 'sports,esports'), ('apex', true, 'sports,esports'),
  ('nhl', true, 'sports,nhl'), ('nfl', true, 'sports,nfl'), ('mlb', true, 'sports,mlb'), ('nba', true, 'sports,nba'),
  ('btc', false, 'crypto,btc'), ('eth', false, 'crypto,eth'), ('sol', false, 'crypto,sol'), ('xrp', false, 'crypto,xrp')
ON CONFLICT (category_key) DO NOTHING;
