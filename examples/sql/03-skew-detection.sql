-- ============================================================================
-- Example: Detecting key skew before it shows up as a long-tail stage
-- ============================================================================
--
-- WHAT THIS DEMONSTRATES
--   How to use simple SQL to measure skew on a join or group-by key, before
--   running a job that will be dominated by a single hot reducer.
--
--   Three techniques are shown:
--     1) Top-N key concentration (what fraction of rows is in the top key?).
--     2) Per-key row count percentiles (median vs p99 vs max).
--     3) Approximate distinct counts to estimate how many reducers will be
--        useful in practice.
--
-- WHY IT MATTERS
--   AQE skew handling helps with already-running queries, but it cannot
--   substitute for actually knowing your key distribution. Skew that
--   develops slowly (one customer growing 100x) is the most common cause
--   of regressions like "the job was fine yesterday".
--
-- WHAT TO LOOK FOR IN SPARK UI WHEN SKEW HITS
--   * Stages tab -> "Summary Metrics" -> max task duration far above the
--     75th percentile.
--   * Stages tab -> per-task metrics -> a single task with massive shuffle
--     read or massive input bytes vs the rest.
--   * Executors tab -> one executor with disproportionate shuffle read.
--
-- PHYSICAL PLAN OPERATORS THAT MATTER
--   * Exchange hashpartitioning(skewed_key) -> the redistribution that
--     becomes hot.
--   * AQE adaptive node showing "skew handling" or "split skewed partitions"
--     when AQE is enabled and detects skew at runtime.
--
-- PRODUCTION ISSUES THIS HELPS DIAGNOSE
--   * "One task takes 45 minutes while the rest finish in 2 minutes."
--   * Executor OOM caused by a single oversized reduce partition.
--   * Output file skew where one Hive/Iceberg partition gets most rows
--     and one tiny part-file gets the rest.
--
-- USAGE NOTES
--   * Run these against a sampled or filtered subset of the table for
--     interactive exploration. Do not use COUNT(*) DISTINCT on a multi-TB
--     table unless you have a reason; prefer approx_count_distinct.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1) Top-N concentration: what fraction of rows is in the top key?
--    Production rule of thumb: if the top key is more than ~5-10% of rows,
--    you have meaningful skew on that key.
-- ----------------------------------------------------------------------------

WITH key_counts AS (
    SELECT
        customer_id,
        count(*) AS n
    FROM events
    WHERE event_date = DATE '2026-04-25'
    GROUP BY customer_id
),
totals AS (
    SELECT sum(n) AS total_rows FROM key_counts
)
SELECT
    customer_id,
    n                                       AS row_count,
    100.0 * n / totals.total_rows           AS pct_of_total
FROM key_counts
CROSS JOIN totals
ORDER BY n DESC
LIMIT 20;

-- ----------------------------------------------------------------------------
-- 2) Distribution percentiles: how uneven is the key load overall?
--    This tells you whether skew is "one bad key" or "the long tail is wide".
-- ----------------------------------------------------------------------------

WITH key_counts AS (
    SELECT
        customer_id,
        count(*) AS n
    FROM events
    WHERE event_date = DATE '2026-04-25'
    GROUP BY customer_id
)
SELECT
    count(*)                                      AS distinct_keys,
    min(n)                                        AS min_rows_per_key,
    approx_percentile(n, 0.50)                    AS p50,
    approx_percentile(n, 0.95)                    AS p95,
    approx_percentile(n, 0.99)                    AS p99,
    max(n)                                        AS max_rows_per_key,
    max(n) / NULLIF(approx_percentile(n, 0.50),0) AS max_to_median_ratio
FROM key_counts;

-- Interpretation:
--   * max_to_median_ratio in the single digits is normal.
--   * Above ~50, expect long-tail tasks.
--   * Above ~500, expect AQE skew join handling to help but not save you,
--     and a single reducer may OOM.

-- ----------------------------------------------------------------------------
-- 3) Approximate cardinality: how many reducers can actually be useful?
--    If you have 200 shuffle partitions but only 15 distinct join keys,
--    most of the 200 partitions will be empty and a few will be huge.
-- ----------------------------------------------------------------------------

SELECT
    approx_count_distinct(customer_id) AS approx_distinct_customers,
    approx_count_distinct(campaign_id) AS approx_distinct_campaigns
FROM events
WHERE event_date = DATE '2026-04-25';

-- ----------------------------------------------------------------------------
-- 4) Once you know the hot keys, you can prove the skew is the cause by
--    excluding them and re-running the slow query. If runtime drops
--    drastically when the top 1-3 keys are filtered out, skew is confirmed.
-- ----------------------------------------------------------------------------

-- Example: rerun the aggregation excluding the top 3 customer_ids found above.
-- SELECT customer_id, count(*) FROM events
-- WHERE event_date = DATE '2026-04-25'
--   AND customer_id NOT IN ('hot_id_1','hot_id_2','hot_id_3')
-- GROUP BY customer_id;
