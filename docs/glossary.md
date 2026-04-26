# Glossary


## Terms

- **Job**: A unit of work triggered by a Spark *action* (e.g. `count`, `collect`, `write`). A single Spark application run can have multiple jobs.
- **Stage**: A set of tasks that can run without needing data from a shuffle boundary. Stages are separated primarily by shuffles (`Exchange`).
- **Task**: The smallest unit of Spark execution: one partition of one stage, executed on one executor core.
- **Narrow transformation**: A transformation where each output partition depends on a small number of input partitions (often 1). Can be pipelined within a stage (e.g. `select`, `filter`).
- **Wide transformation**: A transformation where output partitions depend on many input partitions. Usually forces a shuffle and creates a new stage (e.g. `groupBy`, `join`, `distinct`).
- **Shuffle**: Redistribution of data across executors/partitions (typically by key) so downstream operators can group/join/sort. Expensive due to network IO, disk IO, serialization, sorting, and failure recovery.
- **Partition**: A chunk of a dataset processed by one task. Partition count and sizing control parallelism, per-task working set, and output file shapes.
- **Spill**: Writing intermediate data to local disk when in-memory buffers (sort/agg/join) can’t hold it. Prevents OOM but adds disk IO and slows tasks.
- **Skew**: Uneven data distribution where some partitions/keys are much larger than others, creating long-tail tasks and sometimes OOM.
- **Adaptive Query Execution (AQE)**: Runtime optimization in Spark SQL that can adjust plans after seeing actual sizes (e.g., coalesce shuffle partitions, change join strategies, apply skew handling depending on Spark version/features).
