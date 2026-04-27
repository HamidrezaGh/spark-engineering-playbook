-- Broadcast hash join (default when the build side is small enough).
-- Expect: BroadcastHashJoin, BroadcastExchange, no Exchange on the fact side for the join key.
-- Run with examples/local/run_examples.sh (init creates `events` + `campaigns`).

EXPLAIN FORMATTED
SELECT
    e.event_id,
    e.event_date,
    c.campaign_name,
    c.country
FROM events e
JOIN campaigns c
  ON e.campaign_id = c.campaign_id
WHERE e.event_date = DATE '2026-04-25';
