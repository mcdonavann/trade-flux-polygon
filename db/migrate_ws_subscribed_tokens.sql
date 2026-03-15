-- Migration: add polygon_ws_subscribed_tokens for persisted WS subscriptions.
-- Run: psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f db/migrate_ws_subscribed_tokens.sql

CREATE TABLE IF NOT EXISTS polygon_ws_subscribed_tokens (
  token_id      TEXT PRIMARY KEY,
  subscribed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE polygon_ws_subscribed_tokens IS 'Token IDs we have subscribed to on Polymarket WS; removed when we unsubscribe (e.g. resolved past lookback).';
