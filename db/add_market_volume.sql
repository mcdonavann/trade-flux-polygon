-- Add volume from Gamma API (market-level volume).
ALTER TABLE polygon_markets ADD COLUMN IF NOT EXISTS volume NUMERIC(40, 18);

COMMENT ON COLUMN polygon_markets.volume IS 'Market volume from Gamma API (volumeNum).';
