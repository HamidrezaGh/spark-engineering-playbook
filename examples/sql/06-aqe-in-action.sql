-- ============================================================================
-- Example: Observing what Adaptive Query Execution actually does at runtime
-- ============================================================================
--
-- WHAT THIS DEMONSTRATES
--   AQE rewrites the physical plan at runtime based on real shuffle stats.
--   The three big things it does in production are:
--     1) Coalesce small post-shuffle partitions.
--     2) Switch a planned sort-merge join to a broadcast join when the
--        runtime size of one side is small enough.
--     3) Split skewed shuffle partitions on a sort-merge join.
--
--   This example shows how to confirm each one happened, and how to read
--   the "AdaptiveSparkPlan" / "isFinalPlan" markers in EXPLAIN.
--
-- WHY IT MATTERS
--   "AQE is enabled" tells you nothing. AQE may or may not have triggered.
--   In production, you need to verify what AQE actually did, because:
--     * If AQE coalesced too aggressively, you may end up with too few
--       output partitions and oversized files.
--     * If AQE switched to broadcast unsafely, the driver/executor can OOM.
--     * If AQE skew handling did not trigger, you still have the long-tail
--       stage even though "AQE was on."
--
-- WHAT TO LOOK FOR IN SPARK UI
--   * SQL tab -> the query graph -> the top node will read
--     "AdaptiveSparkPlan" with "isFinalPlan = true" once execution finishes.
--   * Annotations on plan nodes:
--       "CustomShuffleReader coalesced"        -> partition coalescing
--       "CustomShuffleReader local"            -> a shuffle was elided
--       "BroadcastHashJoin"  appearing where the originally planned join
--                            was a SortMergeJoin -> dynamic strategy switch
--       "OptimizeSkewedJoin"                   -> skew handling triggered
--   * Stages tab -> the post-shuffle stage's task count should equal the
--     coalesced partition count, not spark.sql.shuffle.partitions.
--
-- PHYSICAL PLAN OPERATORS THAT MATTER
--   * AdaptiveSparkPlan         -> root marker. Without it, AQE is off or
--                                  not eligible for this query.
--   * CustomShuffleReader       -> the runtime-rewritten shuffle reader.
--   * OptimizeSkewedJoin        -> skew split was applied to one side.
--   * Final BroadcastHashJoin in a plan that originally had SortMergeJoin
--     -> dynamic join strategy switch.
--
-- PRODUCTION ISSUES THIS HELPS DIAGNOSE
--   * "AQE is on but my long-tail stage didn't change" -> skew rules
--     didn't trigger; check the thresholds.
--   * "Output file count dropped from 200 to 4 and now writes are slow"
--     -> AQE coalesced too aggressively; tune target post-shuffle size.
--   * "Driver OOM right before the join" -> AQE switched to broadcast on
--     a side that was not actually safe to broadcast.
--
-- KEY CONFIG KNOBS (Spark 3.x defaults shown; check your runtime)
--   spark.sql.adaptive.enabled                             default true (3.2+)
--   spark.sql.adaptive.coalescePartitions.enabled          default true
--   spark.sql.adaptive.advisoryPartitionSizeInBytes        default 64 MiB
--   spark.sql.adaptive.coalescePartitions.minPartitionNum  cluster-dependent
--   spark.sql.adaptive.skewJoin.enabled                    default true
--   spark.sql.adaptive.skewJoin.skewedPartitionFactor      default 5
--   spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes default 256 MiB
--
-- ASSUMED TABLES
--   events     (large fact, partitioned by event_date)
--   campaigns  (small dimension, ~few thousand rows)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0) Verify AQE is on.
-- ----------------------------------------------------------------------------

SET spark.sql.adaptive.enabled;
SET spark.sql.adaptive.coalescePartitions.enabled;
SET spark.sql.adaptive.skewJoin.enabled;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes;

-- ----------------------------------------------------------------------------
-- 1) AQE coalesce: too many shuffle partitions for a small aggregation.
--    Without AQE, this query produces 200 reduce tasks and 200 tiny output
--    files. With AQE, it should coalesce down to a handful.
-- ----------------------------------------------------------------------------

-- A query that produces a small post-shuffle dataset.
EXPLAIN FORMATTED
SELECT
    event_type,
    count(*) AS n
FROM events
WHERE event_date = DATE '2026-04-25'
GROUP BY event_type;

-- Run the query so AQE can produce a final plan, then re-explain.
SELECT event_type, count(*) AS n
FROM events
WHERE event_date = DATE '2026-04-25'
GROUP BY event_type;

-- After execution, in the Spark UI SQL tab the plan should include:
--   AdaptiveSparkPlan isFinalPlan=true
--    +- CustomShuffleReader coalesced
--       +- ShuffleQueryStage ...
--          +- Exchange hashpartitioning(event_type, 200)
--             +- HashAggregate(... partial_count(1))
--                +- FileScan parquet events ...
--
-- And the Stages tab should report fewer tasks for the final aggregate
-- stage than spark.sql.shuffle.partitions. If the task count is exactly
-- 200, AQE did NOT coalesce (likely because the per-partition advisory
-- size was already met, or AQE is disabled for this query).

-- ----------------------------------------------------------------------------
-- 2) Dynamic switch from sort-merge to broadcast.
--    The query is planned as a sort-merge join because Catalyst's static
--    estimate of the dimension size is above the broadcast threshold.
--    After the dimension's shuffle stage runs, AQE knows the actual size
--    is small and rewrites the join to a broadcast.
-- ----------------------------------------------------------------------------

-- Make sure stats are NOT obviously small so the original plan is SortMergeJoin.
-- (In a real environment you'd skip this; we're forcing the demonstration.)
SET spark.sql.autoBroadcastJoinThreshold = 1;   -- effectively never broadcast statically

EXPLAIN FORMATTED
SELECT /*+ NO_BROADCAST(c) */
    e.event_id,
    e.customer_id,
    c.campaign_name
FROM events e
JOIN campaigns c
  ON e.campaign_id = c.campaign_id
WHERE e.event_date = DATE '2026-04-25'
  AND c.country = 'US';                          -- shrinks campaigns at runtime

-- Run it so AQE can see runtime sizes.
SELECT /*+ NO_BROADCAST(c) */
    e.event_id,
    e.customer_id,
    c.campaign_name
FROM events e
JOIN campaigns c
  ON e.campaign_id = c.campaign_id
WHERE e.event_date = DATE '2026-04-25'
  AND c.country = 'US'
LIMIT 1000;

-- After execution, the final plan's join node should switch to
-- BroadcastHashJoin even though the initial plan was SortMergeJoin.
-- Look in the SQL tab for:
--   AdaptiveSparkPlan isFinalPlan=true
--    +- BroadcastHashJoin [campaign_id], [campaign_id], Inner
--       :- ... events scan ...
--       +- BroadcastQueryStage ...
--          +- ... campaigns scan with country filter ...

-- Reset for the next example.
RESET spark.sql.autoBroadcastJoinThreshold;

-- ----------------------------------------------------------------------------
-- 3) Skew handling on a sort-merge join.
--    This needs the join key to actually be skewed (see 03-skew-detection.sql
--    for how to verify before running). When AQE finds a reduce partition
--    that exceeds skewedPartitionThresholdInBytes AND is more than
--    skewedPartitionFactor * median, it splits the skewed partition into
--    smaller sub-partitions and replicates the small side.
-- ----------------------------------------------------------------------------

-- Force a sort-merge join by disabling broadcast for the demonstration.
SET spark.sql.autoBroadcastJoinThreshold = -1;

EXPLAIN FORMATTED
SELECT
    e.customer_id,
    sum(e.revenue) AS revenue_total
FROM events e
JOIN customer_features f
  ON e.customer_id = f.customer_id
WHERE e.event_date = DATE '2026-04-25'
GROUP BY e.customer_id;

SELECT
    e.customer_id,
    sum(e.revenue) AS revenue_total
FROM events e
JOIN customer_features f
  ON e.customer_id = f.customer_id
WHERE e.event_date = DATE '2026-04-25'
GROUP BY e.customer_id;

RESET spark.sql.autoBroadcastJoinThreshold;

-- After execution, check the SQL tab plan for:
--   AdaptiveSparkPlan isFinalPlan=true
--    +- ... SortMergeJoin ...
--       :- CustomShuffleReader (with skew split markers)
--       +- CustomShuffleReader ...
--
-- And in the Stages tab, the join's reduce stage should show MORE tasks
-- than spark.sql.shuffle.partitions, because skewed partitions were
-- split into multiple sub-partitions. Task duration distribution should
-- be visibly tighter (max / median ratio drops) compared to a run with
-- spark.sql.adaptive.skewJoin.enabled = false.
--
-- If AQE did NOT trigger skew handling and you have a clear long-tail
-- stage, the most common reasons are:
--   * The skewed partition was below skewedPartitionThresholdInBytes.
--   * The skew factor (max / median) was below skewedPartitionFactor.
--   * The join was not a SortMergeJoin (skew handling does not apply to
--     broadcast joins, but those don't have this problem in the first
--     place).
--   * AQE was disabled for the query (e.g., DataSource v2 path on an
--     older runtime).

-- ----------------------------------------------------------------------------
-- 4) Validate AQE outcomes after the run
-- ----------------------------------------------------------------------------
-- Two questions to answer for every AQE-enabled production job:
--   a) Did AQE coalesce, and is the resulting partition count appropriate
--      for the downstream operation (especially writes)?
--   b) Did AQE detect and handle skew, or did it silently do nothing?
--
-- The Spark UI SQL tab is the answer. If the only evidence you have that
-- "AQE worked" is that spark.sql.adaptive.enabled is true, you don't have
-- evidence -- you have a config value.
