# Top Spark Configs (Practical Cheat Sheet)


This page is optimized for interview prep and production reviews: high-leverage configs, what they *really* control, and what to validate in Spark UI.

Also see:

- `docs/field-guides/spark-ui-reading-guide.md`
- `docs/book/12-production-debugging.md`
- `docs/configs/principles.md`

## Shuffle / Wide Operators

- **`spark.sql.shuffle.partitions`**
  - **Intent**: control reduce-side task sizing and parallelism for shuffles.
  - **Validate**: Stages → per-task shuffle read/input size, spill, max task time; avoid tiny-task explosions.

- **`spark.sql.adaptive.enabled` (AQE)**
  - **Intent**: allow runtime plan improvements after Spark sees real sizes.
  - **Validate**: SQL tab shows adaptive plan; Stages show fewer tiny shuffle tasks and stable task sizes.

## Joins

- **`spark.sql.autoBroadcastJoinThreshold`**
  - **Intent**: allow broadcast joins when one side is safely small.
  - **Validate**: SQL tab join becomes broadcast; shuffle join stage shrinks/disappears; Executors remain stable (no GC/OOM spike).

- **`spark.sql.broadcastTimeout`**
  - **Intent**: avoid broadcast timeouts in stressed clusters.
  - **Validate**: timeouts disappear without increasing executor instability.

## Execution / Parallelism

- **`spark.default.parallelism`**
  - **Intent**: default parallelism for RDD operations.
  - **Validate**: Stages task counts and per-task input sizes for RDD-heavy jobs.

## Memory / Stability (YARN/EMR-heavy)

- **`spark.executor.memory`**
  - **Intent**: heap size for executor JVM.
  - **Validate**: reduced spill/GC only if the workload is truly heap-limited.

- **`spark.executor.memoryOverhead`**
  - **Intent**: container memory beyond heap (native/Python/off-heap).
  - **Validate**: YARN container-kill errors stop; executor losses reduce without just masking skew.

- **`spark.executor.cores`**
  - **Intent**: tasks per executor (contention vs throughput tradeoff).
  - **Validate**: Executors GC improves; fewer long-tail tasks from contention; overall utilization remains good.

## Dynamic Allocation / Elasticity

- **Dynamic allocation settings** (platform/version-specific)
  - **Intent**: scale executors with demand to reduce cost.
  - **Validate**: executor churn doesn’t correlate with shuffle failures or cache eviction pain.

## Speculation / Tail Latency

- **Speculation settings** (e.g., `spark.speculation`)
  - **Intent**: reduce tail latency from flaky nodes.
  - **Validate**: helps only when stragglers are not skew-driven; otherwise it doubles expensive work.

## Observability

- **Event log settings** (platform-specific)
  - **Intent**: enable post-run Spark UI and regressions.
  - **Validate**: you can open historical runs and compare dominant stages and plans.

## What Not To Do

- Don’t “tune configs” without first identifying the slow/failed stage and the operator causing it.
- Don’t treat memory increases as a fix for skew.
- Don’t increase parallelism blindly; it can create small files and raise S3 request costs.
