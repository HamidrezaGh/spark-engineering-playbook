# Memory Management

## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- What “memory” means in Spark in practice (heap vs overhead vs Python workers vs off-heap).
- How to distinguish driver OOM vs executor OOM vs YARN container kill.
- What spill indicates and when it is a problem vs a normal pressure-release mechanism.
- Which levers usually help first (reduce per-task working set, adjust cores/partitions, then memory).
- How to find memory pressure evidence in Spark UI + logs (GC time, spill, container kill reasons).

## Core Idea

Spark memory is not just one heap. A production Spark job uses memory for execution, cached data, JVM overhead, Python worker processes, off-heap buffers, shuffle, serialization, and user code. Memory tuning must account for the actual workload shape.

## Key Takeaways

- **Executor heap is only part of memory**; overhead matters heavily on EMR.
- **PySpark often needs more memory overhead** because Python workers run outside JVM heap.
- **Spill is a symptom to measure**, not automatically a failure.
- **Too many concurrent tasks per executor can create memory pressure**.

## Mental Model

Executor memory is the JVM heap available to executor processes. Spark uses a unified memory model where execution memory and storage memory can share space. Execution memory supports joins, aggregations, sorts, and shuffles. Storage memory supports cached or persisted data.

Memory overhead is extra container memory outside the JVM heap. It matters for native memory, off-heap memory, PySpark workers, JVM metadata, thread stacks, and container bookkeeping.

```text
Executor container memory
|-----------------------------------------------------------|
| JVM heap: execution + storage memory                      |
|   - joins, sorts, aggregations, shuffle                    |
|   - cached / persisted blocks                              |
|-----------------------------------------------------------|
| Memory overhead                                            |
|   - Python workers, off-heap, native buffers, thread stack |
|-----------------------------------------------------------|
```

| Symptom | Likely Area | First Evidence |
| --- | --- | --- |
| Java heap OOM | Executor heap | Executor logs, task failure |
| Container killed | Memory overhead or total limit | YARN container reason on EMR |
| High GC | Heap pressure/object churn | Executor GC time |
| Heavy spill | Execution memory pressure | Stage spill metrics |

## What Spark Does Internally

When execution operators need memory, Spark tries to hold working data in memory. If memory is insufficient, supported operators spill to disk. Spill is not automatically a failure; it is how Spark completes work that exceeds memory. But heavy spill usually indicates slower execution and pressure on local disks.

PySpark jobs often need more memory overhead because Python worker processes run outside the executor JVM heap.

## Why It Matters In Production

Memory pressure causes:

- `OutOfMemoryError`.
- `GC overhead limit exceeded`.
- Executor loss.
- Slow spill-heavy stages.
- Container kills from exceeding memory overhead.
- Broadcast join failures.
- Cache eviction and recomputation.

## Common Failure Modes

- Driver OOM from collecting data or planning too many files.
- Executor heap OOM from large joins, aggregations, or cached data.
- Container memory kill from insufficient overhead, common in PySpark.
- Excessive GC from oversized heaps, object-heavy code, or memory churn.
- Disk spill overwhelming local disks.

## Tuning And Configuration

Tune memory together with cores and partitions.

- More executor memory can help large per-task state but can worsen GC if heaps are too large.
- Fewer cores per executor can reduce concurrent task memory pressure.
- More partitions can reduce per-task memory requirements.
- More memory overhead helps PySpark and native/off-heap-heavy workloads.
- Avoid caching unless reuse justifies memory consumption.

Executor sizing is a capacity planning problem: estimate concurrent tasks per executor, memory per task, cache needs, overhead needs, and cluster constraints.

## Spark UI Signals

Check:

- Executor lost events.
- Peak execution memory.
- Spill memory and spill disk.
- GC time.
- Storage tab cache usage.
- Task failure messages.
- Container logs for kill reasons.

## Best Practices

- Reduce data before wide operations.
- Select only required columns.
- Avoid large `collect()` operations.
- Use broadcast joins only when build-side size is safe.
- Monitor spill and GC as production metrics.
- Allocate extra overhead for PySpark.

## Anti-Patterns

- Increasing executor memory without reducing cores or per-task pressure.
- Caching large DataFrames because they "might be reused."
- Treating spill as always fatal.
- Treating spill as always harmless.
- Ignoring Python worker memory in PySpark.

## Example

```python
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "3g")
```

This can be reasonable for a PySpark workload with moderate executor heap needs and significant Python/native overhead. It is not a universal setting.

## Self-check (concept review)

- Explain Spark executor memory.
- What is the difference between execution memory and storage memory?
- What is memory overhead?
- Why do PySpark jobs need more memory overhead?
- What causes `OutOfMemoryError`?
- What causes `GC overhead limit exceeded`?
- What is spill?
- Is spill always bad?
- How do you debug memory pressure?
- How do you decide executor memory size?

## Real Use Case

A PySpark feature engineering job fails on EMR with YARN container memory kills but no Java heap OOM. The executor heap is not the bottleneck; Python workers and Arrow conversion use memory overhead. The fix is to increase `spark.executor.memoryOverhead`, reduce executor cores to lower concurrent Python workers, and reduce wide rows before expensive transformations.
