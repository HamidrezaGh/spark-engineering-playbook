# Small Files Playbook

## Symptom

You see one or more of:

- Jobs are slow with **low CPU** and lots of overhead.
- Query planning is slow (many seconds/minutes before tasks start).
- Reads from S3 feel “chatty” (lots of requests) and costs increase.
- Writes produce tens of thousands of tiny files per table/partition.
- Downstream jobs regress even though their code did not change.

## First Checks

- Confirm file counts and file sizes at the table/partition level.
- Identify whether the pain is on **read**, **write**, or **both**:
  - Read pain: scan stages have huge task counts with small per-task input.
  - Write pain: final stage produces too many output files or commit time dominates.
- Check whether a recent pipeline change introduced:
  - higher partition counts
  - frequent micro-batch writes
  - dynamic partition explosion
  - compaction turned off
- If using Iceberg/Hive-style tables, inspect table metadata (file count per partition, manifest growth).

## Spark UI Signals

Use `docs/field-guides/spark-ui-reading-guide.md` and look for:

- **Stages**
  - scan-heavy stages with extremely large task counts
  - many very short tasks (scheduler overhead dominates)
  - low CPU utilization with lots of wall time
- **SQL**
  - scans that should prune partitions but don’t (missing filters / wrong partitioning)
- **Executors**
  - low executor CPU but long app runtime (often points to listing/IO/planning overhead)

## Likely Causes

- **Over-partitioned writes**: too many shuffle partitions or table partitions leading to many tiny output files.
- **Streaming/micro-batch writes**: frequent small commits create lots of small files.
- **Dynamic partition explosion**: high-cardinality partition columns create many tiny partitions.
- **Lack of compaction**: no periodic rewrite/optimize job to merge small files.
- **Upstream backfills**: historical partitions rewritten into tiny files.

## Remediation Options

- **Prevent at write time**
  - choose a target file size and write strategy that approximates it
  - avoid creating partitions with extremely high cardinality
  - avoid excessive shuffle partitions feeding writes (but validate you don’t create huge tasks)
- **Repair after the fact**
  - run compaction/rewrites for affected partitions
  - for Iceberg-style tables, use table-native rewrite/optimize operations where available
- **Protect downstream**
  - add a guardrail metric: file count per partition, total files added per run, planning time
  - alert on abnormal file count growth after backfills or schema changes

## Real Use Case

A backfill job rewrote 90 days of data and produced ~500,000 Parquet files.

- Downstream jobs slowed dramatically with low CPU usage.
- Spark UI showed scan stages with massive task counts and many short tasks.
- Fix: rewrite/compact the affected partitions to sane file sizes and add a pre-deploy check that blocks backfills that exceed a file-count budget.
