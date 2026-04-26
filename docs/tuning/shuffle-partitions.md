# Shuffle Partitions

## Knob

`spark.sql.shuffle.partitions` sets the default number of reduce-side partitions for Spark SQL / DataFrame shuffles (joins, aggregations, `distinct`, window ops, repartitions) when AQE does not override it.

It is one of the highest-leverage knobs because it controls:

- reduce-side task count (parallelism)
- per-task input size (memory pressure, spill, OOM risk)
- output file count (write shape)
- scheduler overhead (too many tiny tasks)

## When It Helps

- When tasks are too large and spill heavily or OOM: increasing partitions reduces per-task working set.
- When one stage is underutilizing the cluster due to too few tasks.
- When output files are too large for downstream consumers and you need more parallelism (with caution).

## When It Hurts

- Too many partitions create:
  - scheduler overhead (lots of tiny tasks)
  - many tiny output files (especially on writes)
  - higher metadata and commit overhead
- Changing the knob without addressing skew can backfire: you may create more partitions, but the hot key still dominates one partition.
- If the bottleneck is remote storage latency or planning overhead, increasing partitions can make it worse.

## Validation

Validate with Spark UI:

- **Stages**
  - task count vs cores: you want enough tasks to saturate the cluster, but not so many that tasks are tiny
  - per-task **shuffle read/input size**: aim for manageable task sizes (job-dependent)
  - spill and GC time should decrease when you increase partitions for memory pressure cases
  - if max task time remains far above median, you likely still have skew
- **SQL**
  - confirm which operators are shuffling (`Exchange`) and whether AQE coalesced partitions

Practical rule: tune to a stable task size and stable runtime, then set a guardrail (shuffle bytes, spill bytes, output file count) so you detect regressions early.

## Real Use Case

A join+aggregation stage spilled heavily and intermittently failed with executor OOM.

- Spark UI showed very few reduce tasks with huge shuffle read per task.
- Increasing `spark.sql.shuffle.partitions` reduced per-task shuffle read, eliminated spill spikes, and stabilized runtime.
- A follow-up guardrail tracked spill bytes and max task duration for the stage.
