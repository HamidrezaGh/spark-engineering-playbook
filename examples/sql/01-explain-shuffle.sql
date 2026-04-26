-- ============================================================================
-- Example: Identifying shuffle boundaries from a physical plan
-- ============================================================================
--
-- WHAT THIS DEMONSTRATES
--   How to read EXPLAIN FORMATTED output to find:
--     * Where the stage boundary is (Exchange node).
--     * Whether partition and column pruning happened (PartitionFilters,
--       ReadSchema).
--     * Whether partial aggregation reduced shuffle volume.
--     * What the shuffle partition count will be.
--
-- WHY IT MATTERS
--   Most "Spark is slow" investigations start by asking where the shuffles
--   are. EXPLAIN gives you that answer before you even open the Spark UI.
--
-- WHAT TO LOOK FOR IN SPARK UI
--   * SQL tab -> click the query -> the visual plan should show one Exchange
--     node connecting two stages. The Stages tab should report exactly two
--     stages for this query.
--   * Stage 0 (the scan + partial aggregate) should report most of the
--     "Input Size" and the "Shuffle Write" bytes.
--   * Stage 1 (the final aggregate) should report the "Shuffle Read" bytes
--     and produce the final output.
--
-- PHYSICAL PLAN OPERATORS THAT MATTER
--   * FileScan parquet         -> source-side I/O (S3 or HDFS).
--   * PartitionFilters         -> partition pruning was applied.
--   * ReadSchema               -> column pruning was applied.
--   * HashAggregate(... partial_count) -> map-side partial aggregation.
--   * Exchange hashpartitioning -> the shuffle, and the stage boundary.
--   * HashAggregate(... count)  -> reduce-side final aggregation.
--
-- PRODUCTION ISSUES THIS HELPS DIAGNOSE
--   * Missing partition pruning (PartitionFilters is empty when it should
--     filter the partition column) -> full table scan, very expensive on S3.
--   * Missing column pruning (ReadSchema includes too many columns) ->
--     unnecessary I/O and shuffle volume.
--   * Default 200 shuffle partitions on a tiny aggregation -> over-partitioning,
--     scheduler overhead, lots of tiny output files.
--   * Weak partial aggregation (e.g. collect_list instead of count) -> shuffle
--     volume balloons because nothing can be pre-aggregated.
-- ============================================================================

-- Pick a target catalog/database before running this in your environment.
-- USE my_catalog.my_database;

EXPLAIN FORMATTED
SELECT
    customer_id,
    count(*) AS event_count
FROM events
WHERE event_date = DATE '2026-04-25'
GROUP BY customer_id;

-- Expected (simplified) physical plan shape:
--
--   == Physical Plan ==
--   * HashAggregate(keys=[customer_id], functions=[count(1)])
--   +- Exchange hashpartitioning(customer_id, 200)
--      +- * HashAggregate(keys=[customer_id], functions=[partial_count(1)])
--         +- * ColumnarToRow
--            +- FileScan parquet events[customer_id, event_date]
--                 PartitionFilters: [event_date = 2026-04-25]
--                 PushedFilters: []
--                 ReadSchema: struct<customer_id:string>
--
-- Notes:
--   * "Exchange hashpartitioning(customer_id, 200)" is the stage boundary.
--   * 200 comes from spark.sql.shuffle.partitions; AQE may coalesce at runtime.
--   * Skew risk lives in this Exchange: a few hot customer_ids will produce
--     hot reduce partitions.
