# Executor Sizing

## Knob

Executor sizing is the combination of:

- executor count, cores, and memory (e.g., `--num-executors`, `--executor-cores`, `--executor-memory`)
- memory overhead (YARN/containers), often via `spark.executor.memoryOverhead`
- driver sizing (`--driver-memory`) for planning/metadata-heavy workloads

The practical goal is to choose executors that:

- have enough parallelism to saturate the cluster
- have enough memory for the per-task working set (including shuffle/sort buffers and object overhead)
- don’t create excessive GC or waste memory through fragmentation/overhead
- fit the underlying node instance types well (avoid stranded resources)

## When It Helps

- When tasks spill heavily or OOM due to insufficient memory per task, increasing executor memory (or reducing cores per executor) can stabilize workloads.
- When the cluster is underutilized due to too little parallelism, increasing executor count/cores can reduce runtime.
- When GC dominates, reducing executor heap pressure (often fewer cores per executor + right-sized memory) can improve throughput.

## When It Hurts

- Bigger executors are not always better:
  - fewer executors can reduce parallelism and increase tail latency
  - large heaps can increase GC pause times
- Too many cores per executor can increase contention (GC, memory bandwidth) and reduce stability under shuffle pressure.
- Over-allocating memory can waste cluster resources and increase cost without improving runtime.
- Changing executor sizing to “fix” skew often masks the real issue; the hot partition remains hot.

## Validation

Validate with Spark UI:

- **Stages**
  - do you have enough tasks to keep all cores busy?
  - did spill and max task time decrease after changing memory/cores?
- **Executors**
  - GC time: did it improve or worsen?
  - lost executors/task failures: did stability improve?
  - task time distribution: did tail latency improve?

Validation should compare to a known-good baseline: same input size and same data distribution.

## Real Use Case

A shuffle-heavy job had frequent executor losses during reduce-side stages.

- Evidence: high spill and GC, executors dying during peak shuffle.
- Change: reduced executor cores (more, smaller executors) and increased memory overhead to account for off-heap/object overhead.
- Outcome: fewer executor losses, lower tail task times, and more stable runtimes across days.
