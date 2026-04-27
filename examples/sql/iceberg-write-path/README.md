# Iceberg write path and maintenance (SQL templates)

These files are **documentation-first**: they need an **Iceberg**-enabled Spark session, a
**catalog**, and a real table. They are not run by `examples/local/run_examples.sh` by default.

## Files

- [`merge_into.sql`](merge_into.sql) — `MERGE INTO` shape; watch **scan** and **join** cost.
- [`rewrite_data_files.sql`](rewrite_data_files.sql) — compaction / rewrite entry point; schedule,
  not every batch.
- [`write_distribution.md`](write_distribution.md) — `write.distribution-mode` and **sort** order
  when writing to Iceberg.

**See:** [`../../../docs/book/13-iceberg-and-spark.md`](../../../docs/book/13-iceberg-and-spark.md),
[`../../../docs/troubleshooting/iceberg-merge-issues.md`](../../../docs/troubleshooting/iceberg-merge-issues.md), [`../07-iceberg-merge.sql`](../07-iceberg-merge.sql) (runnable in environments with
Iceberg + sample tables).

## Production lesson

**MERGE** is not “free DML” — it is a **read** of candidate files, a **join**, and a **write** of
new files. **Prune** with selective predicates. **Compact** to fix small files **after** you
control **write** layout.

**Common mistake:** `MERGE` with only a `USING` subquery and **no** partition / file **scope** on
the **target** — looks like a small update, runs like a table scan.
