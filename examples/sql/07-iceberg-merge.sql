-- ============================================================================
-- Example: Scoped Iceberg MERGE with snapshot inspection and rollback
-- ============================================================================
--
-- WHAT THIS DEMONSTRATES
--   A production-shaped Iceberg MERGE pattern:
--     * Bound the merge predicate so the target side is partition-pruned.
--     * Use a typed partition predicate (defeating it is the #1 source of
--       runaway merges; see 05-partition-pruning.sql).
--     * Inspect the resulting snapshot via Iceberg metadata tables.
--     * Roll back if the merge published bad data.
--
-- WHY IT MATTERS
--   Iceberg MERGE is one of the most operationally dangerous statements
--   on a large lakehouse table because the cost depends entirely on the
--   ON-clause predicate. A predicate that does not prune the target
--   partitions joins staging against the entire table. This is the exact
--   shape of the failure described in the case study at
--   docs/case-studies/emr-merge-memory-spill.md.
--
-- WHAT TO LOOK FOR IN SPARK UI
--   * SQL tab -> the MERGE query graph -> the scan of the target table
--     should show:
--       PartitionFilters: [event_date = ...]
--     If PartitionFilters is empty, the merge is reading the whole table
--     and you must stop before launching it on a large fact table.
--   * Stages tab -> the join stage backing the MERGE should be the
--     dominant stage. Its shuffle read/write should scale with the
--     bounded partition window, not the table size.
--   * Stages tab -> task duration distribution should be reasonably tight.
--     A long tail here is hot-key skew on the merge key (see 03-skew-
--     detection.sql).
--
-- PHYSICAL PLAN OPERATORS THAT MATTER
--   * IcebergMergeInto / ReplaceData / V2WriteCommand -> the merge writer.
--   * BatchScan IcebergTable[fact_events]
--       runtimeFilters: [...] -> dynamic partition pruning at runtime.
--       PartitionFilters       -> static partition pruning.
--   * Exchange hashpartitioning(<merge_key>, N) -> the join shuffle. This
--     is where shuffle volume and skew live.
--
-- PRODUCTION ISSUES THIS HELPS DIAGNOSE
--   * "MERGE ran for 8+ hours and OOM'd" -> predicate scope was unbounded
--     and the target-side scan pulled in too many partitions.
--   * "MERGE published wrong rows" -> rollback via Iceberg snapshot.
--   * "MERGE produced thousands of tiny files" -> partitioning of the
--     write side is wrong, OR repartition before write was missing.
--
-- ASSUMED TABLES
--   fact_events     -- Iceberg table, partitioned by event_date
--                      columns: event_id, customer_id, event_type,
--                               event_ts, payload, revenue, event_date
--   stg_events      -- Iceberg or Parquet staging for one run_date
--                      columns: same as fact_events for the merge keys
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0) Preflight: how big is the target window we're about to merge into?
--    Always run this BEFORE the MERGE on any non-trivial table.
-- ----------------------------------------------------------------------------

-- Confirm staging is for a single, expected run_date.
SELECT
    min(event_date) AS min_d,
    max(event_date) AS max_d,
    count(*)        AS staging_rows
FROM stg_events;

-- Confirm the target window we plan to touch is the size we expect.
SELECT
    count(*)            AS target_rows_in_window,
    count(DISTINCT event_id) AS target_distinct_events
FROM fact_events
WHERE event_date = DATE '2026-04-25';

-- Confirm key concentration on the merge key in staging (skew preview).
-- See 03-skew-detection.sql for the full skew detector.
SELECT
    event_id,
    count(*) AS n
FROM stg_events
GROUP BY event_id
ORDER BY n DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- 1) The merge itself: bounded scope, typed predicates, partition pruning.
--
--    Important rules encoded below:
--      * The ON clause includes BOTH event_id AND event_date. The
--        event_date equality is what enables partition pruning on the
--        target side. Without it, the target scan reads every partition.
--      * Staging is filtered to the same date so the predicate cannot
--        widen unintentionally.
--      * No function wrapping on event_date (no DATE_FORMAT, no string
--        casting) -- partition pruning would silently break.
-- ----------------------------------------------------------------------------

EXPLAIN FORMATTED
MERGE INTO fact_events t
USING (
    SELECT *
    FROM stg_events
    WHERE event_date = DATE '2026-04-25'
) s
ON  t.event_id   = s.event_id
AND t.event_date = s.event_date
AND t.event_date = DATE '2026-04-25'
WHEN MATCHED THEN UPDATE SET
    customer_id = s.customer_id,
    event_type  = s.event_type,
    event_ts    = s.event_ts,
    payload     = s.payload,
    revenue     = s.revenue
WHEN NOT MATCHED THEN INSERT (
    event_id, customer_id, event_type, event_ts, payload, revenue, event_date
) VALUES (
    s.event_id, s.customer_id, s.event_type, s.event_ts, s.payload, s.revenue, s.event_date
);

-- Things to verify in the EXPLAIN BEFORE running for real:
--   * The fact_events scan reports PartitionFilters: [event_date = 2026-04-25].
--     If not, STOP. The merge will scan the whole table.
--   * ReadSchema on fact_events lists ONLY columns referenced by the
--     ON/UPDATE clauses, not every column.
--   * Exchange hashpartitioning(event_id, N) is present (the join
--     shuffle). Predict shuffle volume from "target_rows_in_window"
--     above.

-- Now run it (in a controlled environment).
MERGE INTO fact_events t
USING (
    SELECT *
    FROM stg_events
    WHERE event_date = DATE '2026-04-25'
) s
ON  t.event_id   = s.event_id
AND t.event_date = s.event_date
AND t.event_date = DATE '2026-04-25'
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- ----------------------------------------------------------------------------
-- 2) Inspect the resulting snapshot via Iceberg metadata tables.
--    Iceberg exposes <table>.snapshots, .history, .files, .manifests, etc.
-- ----------------------------------------------------------------------------

-- The most recent snapshot, with operation, summary metrics, and parent.
SELECT
    snapshot_id,
    parent_id,
    committed_at,
    operation,
    summary['added-records']      AS added_records,
    summary['deleted-records']    AS deleted_records,
    summary['added-data-files']   AS added_files,
    summary['deleted-data-files'] AS deleted_files,
    summary['total-records']      AS total_records_after_commit
FROM fact_events.snapshots
ORDER BY committed_at DESC
LIMIT 5;

-- Linear history of who-wrote-what, useful for incident timelines.
SELECT *
FROM fact_events.history
ORDER BY made_current_at DESC
LIMIT 10;

-- Per-file results from the latest commit, useful when validating that
-- the write produced sensible file sizes (also see file_count_audit.py).
SELECT
    file_path,
    file_size_in_bytes,
    record_count,
    partition
FROM fact_events.files
ORDER BY file_size_in_bytes DESC
LIMIT 20;

-- ----------------------------------------------------------------------------
-- 3) Validation gate before publishing downstream.
--    Run this immediately after the merge. If a check fails, roll back
--    to the previous snapshot (next section) BEFORE consumers read.
-- ----------------------------------------------------------------------------

-- Row count for the affected partition should equal the expected total
-- (existing rows + newly-inserted rows; updates do not change row count).
SELECT
    count(*) AS rows_after_merge,
    count(DISTINCT event_id) AS distinct_event_ids_after_merge
FROM fact_events
WHERE event_date = DATE '2026-04-25';

-- Spot-check: a primary-key-style invariant. Should be zero.
SELECT count(*) AS duplicate_event_ids
FROM (
    SELECT event_id, count(*) AS n
    FROM fact_events
    WHERE event_date = DATE '2026-04-25'
    GROUP BY event_id
    HAVING count(*) > 1
) t;

-- ----------------------------------------------------------------------------
-- 4) Rollback to the previous snapshot if validation fails.
--    Iceberg makes this an atomic metadata operation -- it does not
--    rewrite or move data files.
-- ----------------------------------------------------------------------------

-- Identify the snapshot to roll back to (the parent of the bad snapshot).
SELECT snapshot_id, parent_id, committed_at, operation
FROM fact_events.snapshots
ORDER BY committed_at DESC
LIMIT 5;

-- Roll back. Replace <previous_snapshot_id> with the actual id.
-- CALL my_catalog.system.rollback_to_snapshot('db.fact_events', <previous_snapshot_id>);

-- Or, equivalently in SQL:
-- ALTER TABLE fact_events EXECUTE rollback_to_snapshot(<previous_snapshot_id>);

-- After rollback, re-run the validation queries from section (3) to
-- confirm the table is back to the pre-merge state. Only then is the
-- incident contained; only after that should you redo the merge with
-- the corrected staging or predicate.

-- ----------------------------------------------------------------------------
-- 5) Late updates path (the "21-day window" anti-pattern, done right).
--    If your CDC source can deliver updates outside the run_date window,
--    do NOT widen the daily merge predicate. Instead, run a separate,
--    bounded "late updates" merge on its own cadence and own SLA.
--    See docs/case-studies/emr-merge-memory-spill.md for the full story.
-- ----------------------------------------------------------------------------

-- Example shape (run weekly, not daily):
--
-- MERGE INTO fact_events t
-- USING (
--     SELECT *
--     FROM stg_events_late
--     WHERE event_date BETWEEN DATE '2026-04-05' AND DATE '2026-04-25'
-- ) s
-- ON  t.event_id   = s.event_id
-- AND t.event_date = s.event_date
-- AND t.event_date BETWEEN DATE '2026-04-05' AND DATE '2026-04-25'
-- WHEN MATCHED THEN UPDATE SET *
-- WHEN NOT MATCHED THEN INSERT *;
--
-- Notes:
--   * Even the "late" path is BOUNDED by an explicit date range, not
--     "everything since forever".
--   * The bounded range is repeated in BOTH the staging predicate AND
--     the merge ON clause, so partition pruning fires on the target side.
--   * If your late window is large enough to be expensive, split it into
--     batches of N partitions per run, not one giant merge.
