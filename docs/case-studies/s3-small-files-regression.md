# Case Study — S3 Small Files Regression On A Lakehouse Table

This is an anonymized post-incident review of a daily Spark batch job whose runtime tripled over six months while business logic was unchanged. The cause was small-file accumulation on the source table. Numbers are illustrative; the failure shape is real and common.

## Problem

A daily batch job read a partitioned Iceberg fact table on S3, joined it to a few dimensions, computed daily aggregates, and wrote a derived dataset back to S3. The table was partitioned by `event_date` and had been in production for two years.

The job had been stable for ~14 months at ~22 minutes daily. Over the next six months, runtime drifted up to ~70 minutes. The code had not changed. Input volume per day had grown ~25%, which did not explain a 3× runtime regression.

The team's first response was to upsize the cluster and add executors. Runtime improved by 8 minutes and stopped improving with further scaling. Cost rose ~50% with no proportional benefit.

## Symptoms

- Wall-clock runtime trended from 22 to 70 minutes over six months on a near-flat workload.
- The Spark UI showed an unusually long "scan" stage relative to compute and write stages.
- The Stages tab for the scan stage reported very high task counts (8,000+) with each task processing tiny amounts of data (a few MB or less).
- Driver memory was rising during job startup; the driver was spending 90+ seconds enumerating files before any work began.
- Listing the source table on S3 (`aws s3 ls`) took 4–6 minutes.
- Downstream consumers (a couple of derived jobs and a BI extract) saw similar slowdowns.
- A query that returned in 20 seconds last year now returned in 4–6 minutes for the same partition.

There was no user-visible failure. There was just a slow, expensive job.

## Evidence From Spark UI / File Counts / Scan Planning

### Spark UI — Stages

The scan stage was the bottleneck. Per the Stages tab:

- Number of tasks: ~9,200 for one day's read.
- Median task duration: 1.4 seconds.
- Median input size per task: ~3.8 MB.
- Total scan stage duration: ~28 minutes.

For comparison, a healthy Parquet scan task processes ~64–128 MB and runs for ~10–30 seconds. Tasks of 3.8 MB and 1.4 seconds are scheduler overhead, not work. The cluster was spending most of the scan stage on task setup and Parquet footer reads, not on actual data.

### Spark UI — SQL tab

The SQL tab confirmed:

- `FileScan parquet ... PartitionFilters: [event_date = ...]` — partition pruning was firing.
- `numFiles: 9217` for one partition's read.
- `numPartitions: 9217` (because Spark was creating one read partition per file, since each file was below `spark.sql.files.maxPartitionBytes`).

One partition. 9,217 files. That is small-files.

### File count audit

A quick audit of the source table partitions:

```bash
aws s3 ls s3://lake/events/event_date=2026-04-25/ | wc -l
# 9,217 files

aws s3 ls s3://lake/events/event_date=2026-04-25/ | awk '{ total += $3 } END { print total/1024/1024/1024 " GB" }'
# 142 GB total

# Average file size: 142 GB / 9,217 = ~15 MB per file
```

For comparison, the same partition six months earlier had ~1,000 files averaging ~85 MB. The total bytes were similar; the file count had grown ~9×.

### Driver-side listing

Driver logs showed:

- File listing on the source took ~92 seconds for a one-day partition.
- Iceberg manifest read took an additional ~14 seconds.
- Total planning overhead: ~110 seconds before any executor task started.

For comparison, a year ago: ~6 seconds total planning overhead.

### S3 metrics

S3 server access logs and CloudWatch metrics for the bucket showed:

- Request rate on the source prefix had grown from ~60 req/min to ~1,800 req/min during the scan stage.
- Some 503 SlowDown responses began appearing during peak overlap with other jobs.
- Cost on S3 GET requests for this bucket had grown by ~12× year over year.

## Root Cause

The pipeline producing the source table had been changed eleven months earlier. The original ingest job ran every 30 minutes and produced ~50 files per partition per run (one per Spark task). For a 24-hour day, that meant ~2,400 files per partition before any compaction.

A compaction job had been scheduled to run nightly to rewrite each day's partition into ~50 large files. The compaction job had been running. It had also been silently failing on weekends due to a permissions change six months prior. The on-call rotation had marked the failures as "non-critical" and never debugged them.

So:

- Weekday partitions had ~50 files (compaction succeeded).
- Weekend partitions had ~2,400 files (compaction failed).
- Within weeks, the consumer jobs were reading mostly compacted partitions and feeling fast.
- The compaction job's success rate dropped further over time as the failed weekend partitions accumulated and compaction itself slowed (compacting 2,400 files is slower than compacting 200 files).
- A second contributing factor: the upstream ingest job's parallelism had been doubled six months earlier (from 50 to 100 tasks per run), increasing the per-run file output.
- A third factor: an additional partition column (`event_hour`) had been added to the table in an effort to "reduce skew." This split each daily partition into 24 sub-partitions, multiplying the small-file problem 24×.

By the time the incident was investigated, a typical recent partition had 9,000+ files; older partitions averaged 4,000.

The "obvious" fix (more cluster) did not help because the cluster was waiting on S3 listing and tiny-task scheduling, not on compute.

## Fix

The fix applied four changes in order, validating each before adding the next.

### 1. Run a one-time backfill compaction on the source table

The Iceberg `rewrite_data_files` action was used to compact every partition over the past 13 months:

```sql
CALL system.rewrite_data_files(
    table => 'lake.events',
    where => 'event_date >= DATE \'2025-04-01\'',
    options => map(
        'target-file-size-bytes', '536870912',
        'min-input-files',         '5'
    )
);
```

The compaction was run in batches of ~30 days at a time to avoid overwhelming S3 and the cluster. Total compaction runtime was ~12 hours over a weekend. After this:

- Average files per partition: ~50.
- Average file size: ~2 GB.
- Source table total file count: from ~3.4 million to ~22,000.

The downstream daily job's runtime immediately dropped from 70 minutes to ~24 minutes — back to the original baseline.

### 2. Fix and harden the nightly compaction job

The nightly compaction's permissions issue was diagnosed and fixed. The compaction job was also changed to:

- Run on every day of the week, including weekends.
- Use a target file size of 512 MB.
- Compact only partitions whose file count exceeded a threshold (`min-input-files=5`), avoiding wasted work on already-compacted partitions.
- Emit a metric per run: number of partitions compacted, number of files before/after, total bytes processed.
- Fail loudly with paging on consecutive failures (previously failures were marked non-critical).

### 3. Reverse the over-partitioning by `event_hour`

The decision to partition by `event_hour` was reviewed. It had been made under a misunderstanding — the team thought partitioning by hour would reduce skew. In practice it had multiplied the small-file problem and provided no skew benefit (skew was on `customer_id`, not on time).

The table was repartitioned to drop the `event_hour` column from the partition spec. Iceberg's partition evolution made this a metadata change for new data; existing data was rewritten as part of the compaction backfill.

### 4. Adjust the producer to write fewer, larger files

The upstream ingest job was changed to:

- Run with `coalesce` before write to target ~50 output files per run (instead of one per task).
- Use Iceberg's `distribution-mode='hash'` write property to control output file count.
- Emit the per-run file count metric for monitoring.

The producer-side change reduced the rate at which new small files were created, so the nightly compaction had less work and could finish reliably.

## Result

| Metric | Before | After |
| --- | --- | --- |
| Daily job runtime | 70 minutes | 22 minutes |
| Files per source partition | ~9,200 | ~50 |
| Average source file size | ~15 MB | ~2 GB |
| Driver-side file listing time | ~92 seconds | ~3 seconds |
| Scan stage task count | ~9,200 | ~280 |
| Median task input size | ~3.8 MB | ~80 MB |
| S3 GET request rate during scan | ~1,800 req/min | ~120 req/min |
| Daily S3 cost on this bucket | baseline + 12× | baseline + 1.5× |
| Cluster size | upsized (50% over baseline) | back to baseline |

The job has stayed at its original ~22 minute runtime for over a year since the fix.

## Lessons

The local lesson is "compact your tables." The platform-level lessons are more important.

1. **File count is a first-class table metric.** Every table on the platform should have a per-partition file count metric, a total file count metric, and an alert on growth. Without these, small-files growth is invisible until it is expensive.

2. **Partitioning is a permanent decision; over-partitioning is the most common version of this mistake.** Adding a partition column "for performance" almost never improves performance and almost always multiplies the small-file problem. A table design review should treat any new partition column as a multi-year cost commitment.

3. **Compaction failures are SLA-critical, not best-effort.** A compaction job that fails silently for six months is a production incident in slow motion. Compaction failures should page; their dashboards should be on the on-call's homepage, not buried.

4. **Producer-side file sizing is cheaper than consumer-side compaction.** Writing 50 files per run is cheaper than writing 1,000 and compacting later. The platform should have a default `coalesce` or `distribution-mode` policy for ingest jobs, not leave it to each producer.

5. **"Add cluster" is a diagnostic giveaway.** When a job's runtime regresses and adding executors does not help proportionally, the bottleneck is probably not compute. It is listing, planning, scheduling, or remote storage. Open the Stages tab and look for tasks that are too small.

6. **Slow drift is harder than fast failure.** A 1% per week regression accumulates into a 50% regression over a year and nobody notices until the SLA breaks. Drift detection on runtime, task count, and file count is more valuable than absolute thresholds.

7. **S3 is not free disk.** Every list, every get, every put is a request. Small files multiply requests. Cost shows up in S3 request charges before it shows up in cluster cost. Watch S3 metrics on hot buckets.

## Guardrails Added

- **Per-table file count metric.** Computed nightly for every production table. Surfaced on the data platform health dashboard. Alert fires when a table's average files-per-partition exceeds 200.
- **Compaction success metric.** Per-table per-day compaction success/failure, including bytes processed and file count change. Pages on three consecutive failures.
- **Per-job scan task size metric.** The downstream job emits the median scan task input size as a metric. An alert fires when median falls below 32 MB, which is the small-files signature.
- **Driver listing time metric.** Captured from job startup. Alert fires when listing exceeds 30 seconds, which is usually a small-files signature even when the job still runs.
- **Producer file count metric.** Every ingest job emits files-per-run. Alert on sustained growth.
- **Table design review template.** Adding a partition column requires a written justification, a per-partition file count estimate, and a compaction strategy. Linked from the design review template.
- **Default `distribution-mode='hash'`** on Iceberg writes for the platform's golden-path templates, with an opt-out path that requires review.
- **Runbook update.** A new section "Why is my scan stage suddenly slow?" walks the on-call engineer through the small-files diagnosis path. Linked from the production debugging chapter.
