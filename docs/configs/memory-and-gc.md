# Memory And GC Configs


## Primary Configs

- `spark.executor.memory`
  - **Controls**: JVM heap size for executors.
- `spark.executor.memoryOverhead`
  - **Controls**: container memory beyond heap (native/Python/off-heap). Critical on YARN/EMR.
- `spark.executor.cores`
  - **Controls**: parallel tasks per executor; impacts memory pressure per executor and GC behavior.

## Failure Modes

- Container killed for exceeding memory limits (often fixed by overhead, not heap).
- High GC time leading to low throughput and long tail stages.
- OOM from skew/huge partitions that no amount of “more memory” should hide.

## UI-First Validation

- Executors tab: GC time, executor loss patterns.
- Stages tab: spill and GC time per stage/task, long-tail tasks (skew).
