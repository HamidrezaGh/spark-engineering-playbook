# Shuffle Configs


## Primary Configs

- `spark.sql.shuffle.partitions`
  - **Controls**: default reduce-side partition count for Spark SQL shuffles.
  - **When to change**: tasks too big (spill/OOM) vs too many tiny tasks (overhead).
  - **Validate in Spark UI**: Stages → per-task shuffle read/input size, spill, max task time.

## Often-Related Configs

- `spark.sql.adaptive.enabled` (AQE)
  - **Why it matters**: can coalesce shuffle partitions and reduce overhead after runtime sizes are known.
- `spark.sql.adaptive.coalescePartitions.enabled`
  - **Why it matters**: reduces tiny tasks when upstream tuning is imperfect.

## Failure Modes

- Too few partitions → huge reduce tasks → spill/GC/OOM.
- Too many partitions → scheduler overhead + small output files.
- Skewed keys → long-tail tasks even if partition count is “reasonable”.

## UI-First Debugging Notes

If a shuffle stage is slow and CPU is low, check Stages for:

- high shuffle read with long task durations
- high fetch wait (when available)
- executor loss during the stage (Executors tab)

Then confirm in SQL tab which `Exchange`/operator produced the shuffle.
