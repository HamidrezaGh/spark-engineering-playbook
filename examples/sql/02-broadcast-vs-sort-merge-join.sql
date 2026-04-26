-- ============================================================================
-- Example: Broadcast hash join vs sort-merge join
-- ============================================================================
--
-- WHAT THIS DEMONSTRATES
--   How Spark picks a join strategy, and how to confirm the choice from
--   EXPLAIN. Two queries are shown:
--     1) A join where the dimension table is small enough to broadcast.
--     2) The same join where we explicitly disable broadcasting to force a
--        sort-merge join.
--
-- WHY IT MATTERS
--   Join strategy is the #1 cause of silent performance regressions in
--   Spark. A broadcast join can finish in minutes; the same query with a
--   sort-merge join can run for hours when the fact table is huge.
--
-- WHAT TO LOOK FOR IN SPARK UI
--   * SQL tab -> click the query -> the join node will read either
--     "BroadcastHashJoin" or "SortMergeJoin".
--   * For BroadcastHashJoin: there is no Exchange on the join itself;
--     you'll see a "BroadcastExchange" feeding the small side.
--   * For SortMergeJoin: both sides have an Exchange (hash-partition by the
--     join key), then a Sort, then the join itself. That's two big shuffles.
--   * Stages tab: the stage backing the larger Exchange dominates runtime.
--
-- PHYSICAL PLAN OPERATORS THAT MATTER
--   * BroadcastHashJoin / BroadcastExchange -> small-side broadcast.
--   * SortMergeJoin / Exchange hashpartitioning(join_key) / Sort -> the
--     full shuffle-and-sort pipeline.
--   * Statistics(sizeInBytes=...) -> Catalyst's estimate that drives the
--     decision via spark.sql.autoBroadcastJoinThreshold.
--
-- PRODUCTION ISSUES THIS HELPS DIAGNOSE
--   * "Why did my fast join become slow?" Usually because the small side
--     grew past spark.sql.autoBroadcastJoinThreshold (default 10 MiB) or
--     stats are missing/stale, so Catalyst no longer believes it's small.
--   * Driver / executor OOM caused by an unsafe broadcast (the "small" side
--     wasn't actually small).
--   * Shuffle volume blowing up the wall-clock time on the fact side because
--     the join fell back to sort-merge.
--
-- TUNING NOTES
--   * spark.sql.autoBroadcastJoinThreshold (default 10 MiB) controls the
--     auto-broadcast cutoff. Increase only if you can validate the build
--     side fits safely in executor memory.
--   * /*+ BROADCAST(table) */ hint forces a broadcast when stats are wrong.
--   * Setting the threshold to -1 disables auto-broadcast entirely. Useful
--     for reproducing the sort-merge plan in incidents.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1) Default behavior: small dimension table is broadcast.
-- ----------------------------------------------------------------------------

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

-- Expected operator on the join node when campaigns is small:
--   BroadcastHashJoin [campaign_id], [campaign_id], Inner
--   :- ... events scan with PartitionFilters: [event_date = 2026-04-25]
--   +- BroadcastExchange HashedRelationBroadcastMode(...)
--      +- ... campaigns scan
--
-- No big Exchange on the events side. Cheap.

-- ----------------------------------------------------------------------------
-- 2) Disable broadcast to force sort-merge join.
--    Run this in a session where you can change a SQL conf.
-- ----------------------------------------------------------------------------

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

-- Expected operator on the join node:
--   SortMergeJoin [campaign_id], [campaign_id], Inner
--   :- Sort [campaign_id ASC NULLS FIRST], false, 0
--   :  +- Exchange hashpartitioning(campaign_id, 200)
--   :     +- ... events scan
--   +- Sort [campaign_id ASC NULLS FIRST], false, 0
--      +- Exchange hashpartitioning(campaign_id, 200)
--         +- ... campaigns scan
--
-- Two big shuffles + two sorts. Wall-clock time scales with events size,
-- not campaigns size. This is the "regression" plan.

-- Reset the threshold afterwards in interactive sessions.
RESET spark.sql.autoBroadcastJoinThreshold;

-- ----------------------------------------------------------------------------
-- 3) Force a broadcast with a hint when stats are wrong but you know the
--    small side is safe to broadcast.
-- ----------------------------------------------------------------------------

EXPLAIN FORMATTED
SELECT /*+ BROADCAST(c) */
    e.event_id,
    c.campaign_name
FROM events e
JOIN campaigns c
  ON e.campaign_id = c.campaign_id
WHERE e.event_date = DATE '2026-04-25';
