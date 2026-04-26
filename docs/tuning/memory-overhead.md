# Memory Overhead

## Knob

On YARN/EMR, executor containers need memory beyond the JVM heap:

- JVM off-heap memory
- native memory (compression, I/O, networking)
- Python worker processes (PySpark)
- shuffle buffers and other runtime overhead

The primary knob is:

- `spark.executor.memoryOverhead` (or the platform equivalent) to allocate extra container memory beyond `spark.executor.memory`.

## When It Helps

- When executors are being killed by YARN for exceeding container memory even though the JVM heap isn’t fully used.
- When PySpark workloads spawn Python workers and native memory pushes total usage beyond heap size.
- When shuffle-heavy jobs use additional native/off-heap memory and become unstable.

## When It Hurts

- Setting overhead too high reduces the number of executors you can pack per node and can increase cost or reduce parallelism.
- Using overhead as a substitute for fixing skew or too-large partitions can hide the real issue.

## Validation

Validate by correlating:

- **Spark UI**
  - executor losses/container kills timing vs stages/operators
  - whether spill/GC patterns suggest heap pressure vs container pressure
- **Cluster logs**
  - YARN “container killed for exceeding memory limits” messages

If container kills stop after increasing overhead (without increasing heap), you likely had off-heap/native pressure rather than pure heap OOM.

## Real Use Case

A PySpark ETL job repeatedly lost executors during a shuffle stage with no clear JVM OOM stack trace.

- YARN logs showed containers killed for exceeding physical memory limits.
- Increasing `spark.executor.memoryOverhead` stabilized the job without changing heap size.
- Follow-up: reduce partition sizes and remove unnecessary wide columns to lower total memory footprint.
