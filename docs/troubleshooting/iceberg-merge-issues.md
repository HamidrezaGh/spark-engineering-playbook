# Troubleshooting: Iceberg merge and write issues

**Problem:** `MERGE INTO` is slow, fails with memory/spill, causes small files, or correctness concerns on retries.

## Symptoms

- **MERGE** or **INSERT OVERWRITE** stage dominates runtime; heavy shuffle or wide scans of metadata.
- **OOM** or spill during merge (often looks like a big join in **SQL** tab).
- **Output** file count explodes or never compacts; **snapshots** accumulate.
- **Commit** failures or `CommitFailedException` under concurrent writers.

## What to check first

1. **Match predicate** — is it Selective? Full table **scan** + join for a “small” update is a design smell.
2. **Data file** layout — are target files **large** and **many**, forcing wide reads?
3. **Concurrent** writers — two jobs writing the same table without coordination.
4. **File sizing** and **partitioning** — align target **partition spec** with filter columns.

## Spark UI and SQL

- `MERGE` often compiles to **scan** of matching files + **join** + **write** plan. Look for:
  - Large `BatchScan` / `Iceberg` scan rows.
  - Multiple **Exchange** nodes if join keys or distribution are wrong.
- **Bytes read** from table vs **bytes** in source — a huge ratio suggests **no pruning**.

![Placeholder: EXPLAIN or SQL tab — MERGE into Iceberg with large scan and join subtree](../assets/screenshots/placeholder-explain-physical-plan.png)

Caption: A **wide** `MERGE` plan (large **scan** + **Exchange** on join keys) is the usual signal when the SQL is *logically* small but the engine must **touch** most data files. Narrow predicates and file pruning first, then **rewrite** and maintenance jobs.

## Logs and metrics

- Iceberg **metadata** size (snapshots, manifest lists) — if planning is slow, reduce snapshot retention or use metadata tables to inspect.
- **Commit** stack traces for conflicts.

## Likely causes

- **Update one row, scan whole table** — no partition/file pruning in the merge condition.
- **Target** and **source** both huge without **precomputation** of keys to touch.
- **Write distribution** (partitioning of new files) not aligned with read patterns.
- **Small files** from default Spark write + partition explosion — [small-files](small-files.md).
- **Concurrent** MERGEs — Iceberg **serializable** snapshot isolation; conflicts need retry semantics.

## Fix options

- **Narrow the MERGE** — `AND target.date = '...'` (and similar) so Iceberg **prunes** data files.
- **Stage** change data in a **temp** table, then **merge** with smaller inputs.
- **Sort** and **cluster** (Iceberg `write` options) for downstream read efficiency.
- **rewrite_data_files** and **ExpireSnapshots** (maintenance jobs) — see
  [`../../examples/sql/iceberg-write-path/`](../../examples/sql/iceberg-write-path/README.md).
- **Deduplicate** and **repartition** source before merge to control shuffle.
- For **at-scale** patterns: read [`../patterns/large-iceberg-merge.md`](../patterns/large-iceberg-merge.md).

## Tradeoffs

- **Partitioning** for merge speed can **hurt** ad hoc queries on other columns.
- **Compaction** is CPU and I/O; schedule off-peak.
- **File filters** in merge conditions: wrong predicate can **drop** updates — test carefully.

## Example final diagnosis

*Symptoms:* 3h MERGE, same job used to be 20m. **Cause:** new **source** feed lost `event_date` filter; **scan** read full 18 months. **Fix:** restore date predicate + pre-filter source. **UI:** `Input` on scan stage dropped 95%.

## Prevention checklist

- [ ] MERGE **review** with required partition predicates for large tables.
- [ ] **Idempotent** and **retry** strategy documented for the job.
- [ ] **Maintenance** — compaction and snapshot expiration owned by a schedule.
- [ ] **File count** and **table size** metrics with alerts

**See also:** [`../book/13-iceberg-and-spark.md`](../book/13-iceberg-and-spark.md), [`../book/17-spark-write-path-and-output-files.md`](../book/17-spark-write-path-and-output-files.md), [`../case-studies/emr-merge-memory-spill.md`](../case-studies/emr-merge-memory-spill.md).
