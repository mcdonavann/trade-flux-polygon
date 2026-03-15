-- Fix markets incorrectly marked is_resolved=true due to resolvedBy/closedTime logic.
-- Gamma's closed is authoritative; our 500 came from active=true&closed=false.
UPDATE polygon_markets SET is_resolved = false, resolved_at = null WHERE is_resolved = true;
