# Debugging Index

Use this page when you have an incident symptom and need a fast path to the right Spark UI evidence and playbook.

Also see [`../observability/spark-ui-guide.md`](../observability/spark-ui-guide.md) (canonical UI
guide) and the **decision trees** in [`../troubleshooting/README.md`](../troubleshooting/README.md).
The field guide [`spark-ui-reading-guide.md`](spark-ui-reading-guide.md) is a short index into the
same material.

## Slow Job (But Succeeds)

- **Start here in Spark UI**: Stages → find the slowest stage → inspect task distribution, shuffle, spill, GC.
- **Common causes**:
  - one skewed task (data skew)
  - shuffle-heavy stage (joins/aggregations)
  - scan/planning overhead (small files)
  - join strategy regression (broadcast disabled or no longer possible)
- **Playbook**: `docs/field-guides/debugging-slow-jobs.md`

## One Task Much Slower Than The Rest

- **Start here in Spark UI**: Stages → open the slow stage → compare the slowest task metrics vs median (shuffle read/input/spill/GC).
- **Common causes**:
  - hot key (skew) in join/aggregation
  - hot partition (partition column distribution)
  - uneven input files (one giant file/split)
- **Playbook**: `docs/field-guides/debugging-skew.md`

## OOM / Executor Lost / Container Killed

- **Start here in Spark UI**: Stages → find the stage that started failing → check spill/GC and whether one task is extreme.
- **Common causes**:
  - skewed partition causing one task OOM
  - unsafe broadcast
  - too few partitions (tasks too large)
  - container memory overhead too low (PySpark/native)
- **Playbook**: `docs/field-guides/debugging-oom.md`

## Low CPU But Job Is Slow

- **Start here in Spark UI**: Stages + Executors → confirm low CPU; check whether stages are scan-heavy with many tiny tasks.
- **Common causes**:
  - small files (metadata/listing overhead)
  - waiting on remote storage (S3) or throttling
  - shuffle fetch wait / network bottleneck
  - queue wait / cluster contention (outside Spark)
- **Playbooks**:
  - `docs/field-guides/small-files-playbook.md`
  - `docs/tuning/object-storage.md`
  - `docs/field-guides/debugging-slow-jobs.md`

## Too Many Output Files

- **Start here in Spark UI**: final stage → task count and output files; SQL tab → where partitions are created; confirm partitioning strategy.
- **Common causes**:
  - too many shuffle partitions feeding a write
  - high-cardinality partition columns
  - streaming micro-batches creating tiny files
- **Playbook**: `docs/field-guides/small-files-playbook.md`

## Fetch Failed / Shuffle Read Failures

- **Start here in Spark UI**: the failing reduce-side stage → task failures; Executors tab → lost executors around the failure window.
- **Common causes**:
  - executor loss/disk pressure during shuffle
  - network instability
  - shuffle service issues (platform-specific)
- **Related reading**:
  - `docs/book/02-shuffle-and-performance.md`
  - `docs/book/12-production-debugging.md`

## “It Was Fast Yesterday”

- **Start**: compare to a known-good run (event logs + Spark UI).
- **Common causes**:
  - input growth or new partitions
  - data distribution shift (new skew)
  - file layout regression (small files)
  - config or EMR release change
- **Playbook**: `docs/field-guides/debugging-slow-jobs.md`
