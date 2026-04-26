# Debugging OOM

Status: Draft

## Symptom

The Spark application fails with out-of-memory behavior, commonly:

- Executor OOM / container killed (YARN) during a stage.
- Driver OOM (often during planning, collecting results, or handling huge metadata).
- Repeated task failures with memory errors leading to stage abort.

## First Checks

- Identify **where** the OOM occurred: driver vs executor.
- Identify **when** it occurred: which stage/operator (join/agg/sort/write/UDF).
- Check whether this is:
  - a single hot partition (skew) causing one task to OOM
  - global memory pressure (many tasks spilling/GCing)
  - a broadcast issue (broadcast table too large)
  - too-wide rows / expensive object overhead (common in PySpark)
- Preserve evidence: Spark UI stage metrics, executor logs, and the SQL physical plan.

## Spark UI Signals

Use `docs/field-guides/spark-ui-reading-guide.md` and inspect:

- **Stages**
  - heavy **spill** and **GC time** before failure
  - one task with extreme **input size** or **shuffle read** (skew)
  - stage retries / repeated failed tasks
- **SQL**
  - broadcast joins (and whether the broadcast side is unexpectedly large)
  - large sorts / windows / aggregations that require buffering
  - shuffles (`Exchange`) that create large reduce partitions
- **Executors**
  - high GC time, many failed tasks on a subset of executors
  - lost executors clustered around one stage (disk/memory pressure)

## Likely Causes

- **Skewed partition**: one reduce partition or join key is far larger than others.
- **Too few partitions**: tasks process too much data per partition.
- **Bad broadcast**: broadcast side exceeds memory or triggers driver/executor pressure.
- **Wide rows / object overhead**: many columns, nested structs, high-cardinality maps, Python object overhead.
- **Sort/aggregation memory**: large `groupBy`, `distinct`, window sorts with insufficient memory.
- **Caching too much**: persisting large datasets causes eviction/GC and cascades into OOM.
- **Driver metadata overload**: too many files, huge query plan, large collected results.

## Remediation Options

- **Fix data shape first**
  - address skew (salting/hot-key isolation/AQE skew handling where applicable)
  - increase partitions to reduce per-task working set
  - project fewer columns earlier; avoid wide intermediate rows
- **Fix plan/operator choices**
  - avoid unsafe broadcast; tune broadcast threshold only if you can validate sizes
  - reduce shuffle volume before wide operators (filter early, pre-aggregate)
- **Fix caching usage**
  - remove unnecessary caches; cache only reused datasets that fit
  - choose appropriate storage level; validate eviction behavior
- **Only then adjust resources**
  - executor memory/cores sizing and overhead, but validate you’re not hiding skew

Validation rule: the stage should stop spilling excessively and task memory usage should become stable; if one task still dominates, you likely still have skew.

## Real Use Case

A job started failing with executor OOM during a sort-merge join stage.

- Spark UI showed one reduce task with massive shuffle read and spill.
- The join key distribution changed after a backfill; one key became extremely hot.
- Fix: hot-key handling (salting) plus a pre-filter/projection to reduce shuffle payload; executor memory was left unchanged.
