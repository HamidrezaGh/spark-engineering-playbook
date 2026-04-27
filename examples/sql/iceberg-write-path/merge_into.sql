-- MERGE INTO template (Iceberg) — adjust catalog, database, and predicates.
-- Replace placeholders before running. Validate EXPLAIN: scan should be narrow, not a full table
-- read for a "small" update.

-- MERGE INTO prod.analytics.facts t
-- USING src_updates s
-- ON  t.id = s.id
-- AND t.dt = DATE '2026-04-25'    -- keep partition predicate so Iceberg can prune
-- WHEN MATCHED AND s.op = 'U' THEN UPDATE SET ...
-- WHEN MATCHED AND s.op = 'D' THEN DELETE
-- WHEN NOT MATCHED THEN INSERT *;

-- EXPLAIN
-- MERGE INTO ...
