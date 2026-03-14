-- Reset subgraph backfill cursor so next run starts from POLYGON_SUBGRAPH_MIN_TIMESTAMP (e.g. 2026-03-13 05:00 UTC).
-- Run once: psql -h HOST -U USER -d tradeflux -f db/reset_subgraph_cursor.sql
DELETE FROM sync_state WHERE job_name = 'polygon_trades';
