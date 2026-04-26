-- ============================================================================
-- Example: Verifying partition pruning and column pruning from EXPLAIN
-- ============================================================================
--
-- WHAT THIS DEMONSTRATES
--   How to read a physical plan to confirm that two of the most important
--   scan-time optimizations are actually happening:
--     * Partition pruning  -> only the relevant partitions are listed/read.
--     * Column pruning     -> only the projected columns are read.
--   And what the plan looks like when each one is silently broken.
--
-- WHY IT MATTERS
--   On S3-backed tables, scan cost is dominated by listing and bytes read.
--   A query that "looks fine" can read 100x more data than it needs if
--   pruning is not happening, and the only place this is visible is the
--   physical plan. Spark UI input bytes will tell you the bytes are big;
--   only the plan tells you why.
--
-- WHAT TO LOOK FOR IN SPARK UI
--   * SQL tab -> click the query -> the FileScan node should display:
--       PartitionFilters: [<your partition predicate>]
--       PushedFilters:    [<predicates pushed into the file format>]
--       ReadSchema:       struct<only_the_columns_you_select>
--   * Stages tab -> the scan stage -> "Input Size / Records" should be
--     proportional to the partitions that survived pruning, not the whole
--     table. If it is not, pruning is not working.
--
-- PHYSICAL PLAN OPERATORS THAT MATTER
--   * FileScan parquet ...
--       PartitionFilters -> partition pruning at planning time.
--       PushedFilters    -> predicates pushed into Parquet/ORC reader.
--       DataFilters      -> predicates that will be re-evaluated by Spark
--                           after the row is read. Useful but cheaper if
--                           they can be pushed down instead.
--       ReadSchema       -> column pruning. Should match SELECT projection.
--
-- PRODUCTION ISSUES THIS HELPS DIAGNOSE
--   * "Why is this query reading the whole table?" -> partition predicate
--     is wrapped in a function that defeats pruning, or the partition
--     column type was inferred wrong (e.g. string vs date).
--   * "Why does selecting one column take as long as SELECT *?" -> column
--     pruning is being defeated, often by a UDF or a wide intermediate.
--   * "Why is the metastore call slow?" -> too many partitions surviving
--     pruning. Listing cost on S3 scales with surviving partition count.
--
-- ASSUMED TABLE
--   events (
--     event_id      string,
--     customer_id   string,
--     event_type    string,
--     event_ts      timestamp,
--     payload       struct<...>,    -- wide column, expensive to read
--     event_date    date            -- partition column
--   )
--   PARTITIONED BY (event_date)
--   STORED AS PARQUET
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1) GOOD: pruning is visible in the plan.
-- ----------------------------------------------------------------------------

EXPLAIN FORMATTED
SELECT customer_id, event_type
FROM events
WHERE event_date = DATE '2026-04-25';

-- Expected scan node (simplified):
--   FileScan parquet events[customer_id, event_type, event_date]
--     PartitionFilters: [isnotnull(event_date), (event_date = 2026-04-25)]
--     PushedFilters:    []
--     ReadSchema:       struct<customer_id:string,event_type:string>
--
-- Checks:
--   * PartitionFilters mentions event_date.
--   * ReadSchema lists ONLY the columns we projected (no `payload`).
--   * Spark UI input bytes for this stage should equal the bytes of one
--     day's partition, not the whole table.

-- ----------------------------------------------------------------------------
-- 2) BAD: predicate wrapped in a function defeats partition pruning.
--    Common in production code; usually unintentional.
-- ----------------------------------------------------------------------------

EXPLAIN FORMATTED
SELECT customer_id, event_type
FROM events
WHERE date_format(event_date, 'yyyy-MM-dd') = '2026-04-25';

-- Expected scan node (simplified):
--   FileScan parquet events[...]
--     PartitionFilters: []                  -- !!! pruning lost
--     PushedFilters:    []
--     DataFilters:      [(date_format(event_date, yyyy-MM-dd) = 2026-04-25)]
--
-- Why: Spark cannot evaluate `date_format(event_date, 'yyyy-MM-dd')`
-- against partition metadata at planning time, so it cannot prune. Every
-- partition will be listed and opened.
--
-- Fix: compare event_date directly to a date literal:
--   WHERE event_date = DATE '2026-04-25'

-- ----------------------------------------------------------------------------
-- 3) BAD: implicit type cast defeats partition pruning.
--    Happens when partition column is DATE but the predicate is STRING.
-- ----------------------------------------------------------------------------

EXPLAIN FORMATTED
SELECT customer_id
FROM events
WHERE event_date = '2026-04-25';   -- string literal, partition col is DATE

-- Spark may insert a cast that defeats pruning depending on version:
--   PartitionFilters: []
--   DataFilters:      [(cast(event_date as string) = 2026-04-25)]
--
-- Fix: use a typed literal -> DATE '2026-04-25', or to_date('2026-04-25').
-- Modern Spark + AQE usually handles this; older runtimes do not. Read
-- the plan, do not assume.

-- ----------------------------------------------------------------------------
-- 4) GOOD: predicate pushdown into the file format.
--    Even within a partition, Parquet can skip row groups using min/max
--    statistics IF the predicate is pushed down.
-- ----------------------------------------------------------------------------

EXPLAIN FORMATTED
SELECT customer_id, event_type
FROM events
WHERE event_date = DATE '2026-04-25'
  AND event_type IN ('click','purchase');

-- Expected scan node:
--   FileScan parquet events[...]
--     PartitionFilters: [event_date = 2026-04-25]
--     PushedFilters:    [In(event_type, [click,purchase])]
--     ReadSchema:       struct<customer_id:string,event_type:string>
--
-- The In(...) predicate is now in PushedFilters, meaning Parquet can use
-- column statistics to skip whole row groups before decompressing them.

-- ----------------------------------------------------------------------------
-- 5) BAD: a UDF on a partition or filter column blocks pushdown.
-- ----------------------------------------------------------------------------

-- Suppose there is a SQL UDF: my_normalize_type(t) -> string.
-- Then this query cannot push the type filter:
--
-- EXPLAIN FORMATTED
-- SELECT customer_id
-- FROM events
-- WHERE event_date = DATE '2026-04-25'
--   AND my_normalize_type(event_type) = 'click';
--
-- Expected scan node:
--   PartitionFilters: [event_date = 2026-04-25]      -- still ok
--   PushedFilters:    []                              -- !!! lost
--   DataFilters:      [(my_normalize_type(event_type) = click)]
--
-- The UDF predicate is evaluated row-by-row on the executor. Spark cannot
-- push it into Parquet because the engine has no idea what the UDF does.
--
-- Fix: rewrite as a built-in expression, or normalize the column upstream
-- so the filter can be a plain equality.

-- ----------------------------------------------------------------------------
-- 6) Checklist when reviewing a slow scan
-- ----------------------------------------------------------------------------
--   1. Does PartitionFilters mention every partition predicate I wrote?
--   2. Does ReadSchema include ONLY the columns I projected?
--   3. Does PushedFilters include selective predicates (equalities, IN,
--      ranges) on non-partition columns?
--   4. Are any UDFs sitting in DataFilters that could be rewritten as
--      built-ins?
--   5. Does the Spark UI input size match the partitions that survived
--      pruning, or the full table?
--
-- Answer "yes" to 1-3 and "no" to 4 and your scan is healthy. Anything
-- else is an optimization gap that the rest of the chapters in this book
-- are written to help you fix.
