# Debugging Slow Jobs

## Symptom

The job still succeeds, but runtime regresses (often 2×–20×) compared to a known-good baseline, or it intermittently breaches an SLA.

## First Checks

- Identify what changed since the last good run: code, config, input size, data distribution, cluster size/type, EMR release, dependencies, schedule/concurrency.
- Confirm where time went: queue wait vs Spark runtime vs write commit time.
- Find the slowest stage in Spark UI and check whether the regression is dominated by:
  - shuffle
  - scan/planning
  - join strategy change
  - spill/GC
  - remote storage latency (S3)
  - skew
- Capture an `EXPLAIN` (or Spark UI SQL physical plan) for today and for a known-good run.

## Spark UI Signals

Start with `docs/field-guides/spark-ui-reading-guide.md` and focus on:

- **Stages**
  - 1–2 stages dominate runtime.
  - Long tail of task durations (skew).
  - High shuffle read/write (shuffle-bound).
  - High spill and GC time (memory pressure).
  - Very large task count with tiny inputs (scheduler overhead).
- **SQL**
  - `Exchange` operators (shuffle boundaries).
  - Join strategy (broadcast vs sort-merge) and whether it changed.
  - Missing partition pruning or filter pushdown.
  - AQE: whether partitions were coalesced or skew handling applied.
- **Executors**
  - Lost executors, high GC, uneven shuffle read/write per executor.

## Likely Causes

- **Input growth**: more partitions/files/rows than baseline; late partitions arriving.
- **Data distribution change**: new skew on a key or partition.
- **Join strategy regression**: broadcast disabled, thresholds changed, stats missing, or a table got bigger.
- **Small files**: scan planning is slow, task overhead is high, S3 listing dominates.
- **Partition sizing**: too few partitions (huge tasks) or too many partitions (scheduler overhead, tiny files).
- **Remote storage behavior**: S3 throttling/errors, slow reads, excessive HEAD/LIST calls.
- **CPU-heavy code paths**: UDFs, JSON parsing, compression changes, expensive projections.
- **Cluster contention**: YARN queue saturation, spot/preemption, noisy neighbor.

## Remediation Options

- **Prove the bottleneck first**, then pick the smallest fix you can validate:
  - If shuffle-bound: reduce shuffle input (filter/project earlier), change join strategy, tune shuffle partitions, enable/validate AQE.
  - If skew: skew handling (AQE skew features where applicable, salting, pre-aggregation, hot-key isolation).
  - If small files: compact upstream, optimize table layout, reduce file count per partition, use sane write sizing.
  - If under-partitioned: increase partitions to keep task sizes manageable.
  - If over-partitioned: reduce partitions to cut task overhead and output file count.
  - If CPU-bound: remove/replace UDFs, push computation into Spark SQL, reduce parsing, adjust serialization.
  - If platform-bound: validate S3 errors/throttling, EMR health, YARN queue wait, instance failures.
- Add a **guardrail metric** after the fix: shuffle bytes, spill bytes, top-key concentration, file count, or stage runtime percentiles.

## Real Use Case

A job was “stable” at ~30 minutes and suddenly became 3 hours with no code change.

- Spark UI showed low CPU usage and long stage time dominated by scans with many short tasks.
- The source table received a backfill that created hundreds of thousands of small files.
- Fix: compact files (or rewrite table partitions) and add an alert on file count per partition plus scan planning time.
