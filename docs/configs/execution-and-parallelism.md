# Execution And Parallelism Configs

Status: Draft

## Core Idea

Parallelism controls how many tasks can run concurrently and how large each task’s working set becomes.

## Primary Configs

- `spark.default.parallelism`
  - **Controls**: default parallelism for RDD-based operations (not Spark SQL shuffles).
  - **Validate**: Stages task counts and per-task input sizes for RDD-heavy jobs.

- `spark.sql.shuffle.partitions`
  - Covered in `docs/configs/shuffle.md` (Spark SQL parallelism for shuffle stages).

## Failure Modes

- Too few tasks → poor cluster utilization, huge tasks, spill/OOM.
- Too many tasks → scheduler overhead and small-file explosion on writes.

## UI-First Validation

- In Stages:
  - task count should be high enough to keep cores busy
  - task durations should not be dominated by scheduler delay
  - per-task input/shuffle read should be in a manageable range for your workload
