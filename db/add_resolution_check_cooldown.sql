-- Cooldown for resolution refresh: avoid re-enqueueing same market within cooldown window.
-- Eligibility: is_resolved=false AND (not in cooldown OR last_check_at + cooldown elapsed).

CREATE TABLE IF NOT EXISTS polygon_resolution_check_cooldown (
    slug TEXT PRIMARY KEY,
    last_check_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    check_count INT DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_polygon_resolution_check_cooldown_last_check
    ON polygon_resolution_check_cooldown(last_check_at);

COMMENT ON TABLE polygon_resolution_check_cooldown IS
    'Tracks when each market slug was last enqueued for resolution refresh. Cooldown prevents re-publishing too soon.';
