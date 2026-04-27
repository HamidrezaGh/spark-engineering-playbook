-- Sort-merge join: force by disabling auto-broadcast (compare to broadcast_join.sql).
-- Expect: SortMergeJoin, two Exchange hashpartitioning nodes, two Sort nodes.
-- RESET the conf after interactive use if needed.

SET spark.sql.autoBroadcastJoinThreshold = -1;

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

RESET spark.sql.autoBroadcastJoinThreshold;
