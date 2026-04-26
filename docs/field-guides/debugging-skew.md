# Debugging Skew


## Symptom

One stage (usually a shuffle stage) has a long tail: most tasks finish quickly, but a small number of tasks run far longer, often causing the whole job to stall behind a single straggler.

## First Checks

- Confirm it’s skew, not random slowness:
  - Is the slow stage dominated by a few very slow tasks?
  - Do the slow tasks have much larger input/shuffle read/spill than the median?
- Identify the operator causing the skew:
  - Open the slow stage in **Stages**.
  - Map it to the operator in **SQL** (often an `Exchange` feeding a join/agg).
- Identify the skew dimension:
  - join key distribution (hot keys)
  - partition column distribution (hot partitions)
  - write partition distribution (one partition gets most rows)
  - upstream file layout (one file/split much larger)

## Spark UI Signals

Use `docs/field-guides/spark-ui-reading-guide.md` and look for:

- **Stages → task duration distribution**: very high max compared to median.
- **Stages → per-task metrics** (slow tasks vs typical tasks):
  - much higher **shuffle read** or **input size**
  - heavy **spill**
  - high **GC time**
- **Executors**:
  - one executor shows disproportionately high shuffle read/write or task time.
- **SQL plan**:
  - skew typically shows up at operators that force redistribution: joins/aggregations/window/sort.

## Likely Causes

- **Hot keys**: a small number of keys account for a large fraction of rows (common in aggregations and joins).
- **Join amplification**: skewed key joins to many rows on the other side (e.g., one customer joins to millions of events).
- **Skewed partitions**: table partitioning or ingestion creates uneven partition sizes.
- **Skew introduced by filters**: filtering can turn an even distribution into a hot partition/key.
- **Upstream file skew**: a small number of very large files/splits dominate task input.

## Remediation Options

Pick the smallest safe option that matches the operator:

- **If it’s a join**
  - Ensure the “small side” is truly small and broadcast-safe; broadcast can eliminate a shuffle join.
  - If broadcast is unsafe: handle hot keys (salting, hot-key isolation, pre-aggregation).
  - With AQE enabled: validate whether Spark applied skew join handling (and whether it helped).
- **If it’s an aggregation**
  - Pre-aggregate earlier (reduce rows before the skew point).
  - Salt hot keys so the heavy key is split across multiple reducers, then merge.
- **If it’s a write**
  - Rebalance before write (but validate you didn’t create a worse shuffle).
  - Change partitioning strategy to avoid one hot partition.
- **If it’s file/input skew**
  - Compact or rewrite upstream partitions; avoid huge single files mixed with many tiny files.

Validation rule: after the fix, the slow stage should show a tighter task duration distribution and reduced max task time. If overall shuffle bytes increased significantly, you may have traded skew for a bigger shuffle cost.

## Real Use Case

A daily merchant-level aggregation had one task taking 45 minutes while the rest finished in 2 minutes.

- Spark UI showed the slow task had ~30× the shuffle read and heavy spill.
- A key-frequency sample showed one merchant accounted for ~40% of rows.
- Fix: split hot merchant into salted sub-keys for the heavy aggregation stage, then recombine, plus alert on top-1 key concentration.
