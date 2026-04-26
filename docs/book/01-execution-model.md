# Execution Model


## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- What executes where (driver vs executors)?
- What is a job vs stage vs task?
- What creates a new stage (and why)?
- When Spark “reads/writes”, what storage layer is actually involved (source/sink vs shuffle)?
- Where do you look first in the Spark UI when something is slow or failing?

If you need the deep mechanics and tuning specifics of shuffle, use `docs/book/02-shuffle-and-performance.md` as the next chapter.

## Core Idea (One Paragraph)

Spark turns a logical data computation into distributed work. A user writes transformations and actions; Spark builds a lineage graph, optimizes it, splits it into stages at shuffle boundaries, and runs many tasks across executors.

The useful production model is:

- **Job**: work triggered by one action.
- **Stage**: a set of tasks that can run without waiting for a shuffle boundary.
- **Task**: one unit of work over one partition of data.

Note: shuffles are best thought of as **between stages**, but tasks on both sides participate. The upstream stage performs **shuffle write** (partitioning + writing shuffle files); the downstream stage performs **shuffle read** (fetching those files over the network/disk) and then continues processing.

Example: `df.groupBy("k").count()` typically becomes:

```text
Stage 0 (shuffle write): scan/filter/map -> write shuffle buckets by k
Stage 1 (shuffle read):  read shuffle buckets -> aggregate counts
```

## Glossary (Practical, Not Theoretical)

- **Transformation**: builds a plan (lazy). Examples: `select`, `filter`, `withColumn`, `join`, `groupBy`.
- **Action**: triggers execution. Examples: `count`, `collect`, `show`, `write`.
- **Partition**: the unit of parallelism for a dataset; tasks operate on partitions.
- **Narrow dependency**: each output partition depends on a small number of input partitions (pipeline-friendly).
- **Wide dependency**: output partitions depend on many input partitions (requires redistribution → usually a shuffle).
- **Shuffle**: distributed redistribution of intermediate data (local disk + network), typically introducing a stage boundary. For deeper details and tuning, see `docs/book/02-shuffle-and-performance.md`.

## What Runs Where (Driver vs Executors)

- **Driver**:
  - Builds and optimizes the plan (Spark SQL Catalyst planning happens here).
  - Breaks work into stages and tasks; schedules tasks.
  - Tracks metadata, progress, and collects action results.
- **Executors**:
  - Run tasks (read input, compute, write output).
  - Store cached data and shuffle files on local disks.
  - Failures are usually retried at the task level (with important caveats for shuffle blocks).

Production rule: **driver memory** is where `collect()`/`toPandas()` hurts; **executor memory/disk/network** is where shuffle-heavy operators hurt.

## Where Data Is Actually Read/Written (Source/Sink vs Shuffle)

In Spark docs and UIs you'll often see "read" and "write" as generic terms. In production it helps to separate **shuffle I/O** from **source/sink I/O**, because they hit different storage layers.

- **Source read / sink write**: your input and output paths, typically:
  - **S3** (common on EMR): `s3://...` (or `s3a://...`) via the Hadoop filesystem layer.
  - **HDFS** (if enabled on the cluster): `hdfs://...`.
  - **Local filesystem** on nodes: `file:/...` (mostly for scratch, not durable storage).
- **Shuffle read / shuffle write**: Spark's intermediate data exchange between stages.
  - **Where it lives**: executor-local disks (instance store / attached volumes) as shuffle files.
  - **How it moves**: downstream tasks fetch shuffle blocks over the network from the executors that produced them.

Note: in a shuffle, data for a given "bucket" (shuffle partition) is produced on many executors, then the reduce task for that bucket fetches blocks from other executors over the network so it can finish the aggregation/join.

Note: shuffle files are **not** written to HDFS/S3; HDFS/S3 are used when your *source/sink* paths are `hdfs://...` or `s3://...` (i.e., when you explicitly read/write datasets there).

Practical implication: "shuffle write is high" does *not* mean you are writing to S3/HDFS; it usually means you are writing temporary shuffle files to executor disks and saturating disk + network.

## Key Takeaways

- **Actions create jobs**; transformations only build the plan.
- **Shuffle boundaries create new stages** because data must be redistributed.
- **Tasks are partition-level work**, so partition sizing controls parallelism and pressure.
- **Driver failure usually fails the application**; executor failure is usually retried.

## The Execution Flow (The Mental Model To Debug With)

Transformations such as `select`, `filter`, and `withColumn` are lazy. They describe a plan but do not execute it. Actions such as `count`, `collect`, `write`, and `show` force Spark to materialize the plan.

Spark builds a DAG. Narrow dependencies can be pipelined because each output partition depends on a small number of input partitions. Wide dependencies require data from many upstream partitions to be redistributed, usually by key. That redistribution creates a shuffle and normally creates a new stage.

```text
User code
  -> logical plan
  -> optimized plan
  -> physical plan
  -> job
      -> stage 1: narrow work
      -> shuffle boundary
      -> stage 2: reduce-side work
          -> tasks per partition
```

| Concept | Created By | Production Signal |
| --- | --- | --- |
| Job | An action such as `count`, `write`, or `collect` | End-to-end application work |
| Stage | A chain of work split by shuffle boundaries | Shuffle-heavy stages dominate runtime |
| Task | One partition of one stage | Skew shows up as slow or large tasks |

## What Actually Creates A New Stage (Common Triggers)

This is the “why did Spark split here?” cheat sheet:

- **`Exchange` in the physical plan** (Spark SQL / DataFrames) almost always implies a shuffle and therefore a stage boundary.
- **Wide transformations** usually create a stage boundary: `groupBy`, most `join`s (unless broadcast), `distinct`, many window operations, global sort, `repartition`.
- **Writes** can introduce additional stages for sorting, bucketing, or commit/rename semantics depending on format and connector behavior.

If you’re unsure, go to **Spark UI → SQL tab** and find `Exchange` nodes; then correlate to **Spark UI → Stages tab**.

## What Spark Does Internally (High Level)

When an action like `count()` runs, Spark:

1. **Logical planning**: Builds or reuses the logical plan.
2. **Analysis**: Analyzes column and table references.
3. **Logical optimization**: Optimizes the logical plan.
4. **Physical planning**: Chooses one or more physical plans.
5. **Stage planning**: Breaks the physical plan into stages.
6. **Task scheduling**: Schedules tasks for each stage.
7. **Execution**: Runs tasks on executors.
8. **Result/commit**: Returns a result to the driver or commits output.

Note: Spark SQL's **Catalyst** engine runs on the **driver** during planning—primarily in **Analysis** (the Analyzer) and **Logical optimization** (the Optimizer), and it also contributes to **Physical planning**.

The number of tasks in a stage is usually driven by the number of partitions in that stage. For file scans, input splits and file partitioning matter. For shuffle stages, settings such as `spark.sql.shuffle.partitions` and Adaptive Query Execution matter.

## Why It Matters In Production

Most Spark failures and performance problems are easier to understand when you know whether the bottleneck is at the job, stage, or task level.

- `Job-level issue`: the full application is slow or failing.
- `Stage-level issue`: one shuffle, join, aggregation, or write dominates runtime.
- `Task-level issue`: skew, bad partition sizing, slow nodes, spill, or executor instability.

The driver owns scheduling, plan coordination, metadata, and result collection. If the driver dies, the Spark application usually fails. Executors run tasks and store shuffle or cached data. If an executor dies, Spark can usually retry its tasks, but shuffle data stored on that executor may need to be recomputed unless an external shuffle service or shuffle tracking is available.

## Common Failure Modes

- Driver OOM from `collect()`, huge query plans, too much metadata, or too many files.
- Executor loss from memory pressure, disk pressure, node failure, container preemption, or network instability.
- Long-tail stages where one task is much slower than peers.
- Excessive task overhead from too many tiny partitions.
- Underutilized cluster from too few partitions.

## Production Debug Loop (What To Do First)

Tune only after identifying the bottleneck. This loop is intentionally repetitive because it works:

- **Find the bottleneck**:
  - **Spark UI → Jobs**: which action/job dominates runtime?
  - **Spark UI → Stages**: which stage dominates? Compare **median vs max task time** (long tails matter).
  - **Spark UI → SQL** (DataFrames/SQL): where are the `Exchange` nodes and what join strategy is chosen?
  - **Spark UI → Executors**: are executors dying, GC heavy, or spilling?
  - **Logs**: confirm OOMs, fetch failures, retries, timeouts.
- **Classify the stage** (typical signal → typical cause):
  - **High shuffle read/write** → wide transformation; check skew and partition sizing. Deep-dive: `docs/book/02-shuffle-and-performance.md`.
  - **Max task time ≫ median** → skew or bad nodes; open the stage’s task list and look for a few huge tasks.
  - **High spill** → per-task working set too large; reduce data earlier or change join strategy.
  - **High input read time/bytes** → scan bound; check layout (small files), partition pruning, pushdown.
  - **Lots of tiny tasks + scheduler delay** → overhead bound; reduce partitions / `coalesce`.
  - **Many failed/retried tasks** → instability bound; fix executor loss/disk/network before micro-tuning SQL.
- **Change one thing** and re-check the same stage metrics (duration, max task time, shuffle bytes, spill, input bytes).

## Tuning And Configuration (Only After You’ve Classified The Bottleneck)

- Increase parallelism when tasks are too large and cluster cores are idle.
- Reduce partition count when task overhead dominates.
- Avoid collecting large data to the driver.
- Use Adaptive Query Execution for runtime shuffle coalescing and skew mitigation.
- Size the driver for metadata-heavy workloads, especially jobs scanning many files or generating large plans.

## Spark UI Signals

Use the Spark UI to locate the level of the problem:

- Jobs tab: which action triggered work and how long it took.
- Stages tab: task counts, shuffle read/write, spill, locality, and long tails.
- SQL tab: physical operators, joins, exchanges, adaptive plan changes.
- Executors tab: failed tasks, memory use, disk spill, GC time, and executor loss.

## Hadoop + YARN (What They Do On EMR)

On EMR, Spark usually runs on top of the Hadoop stack (even when your data lives in S3).

- **Hadoop filesystem layer**: Spark uses Hadoop FS implementations for `s3://`, `hdfs://`, and `file:/`. Many "S3 read/write" behaviors are connector behaviors (retries/timeouts, listing, commit semantics).
- **YARN** (for YARN-based clusters): starts driver/executors as containers and enforces resource limits. Many Spark failures show up as YARN container/application failures.
- **HDFS** (optional): a cluster filesystem backed by node disks—often used for fast, in-cluster temporary data; durability/lifecycle depends on your EMR setup (many treat it as more ephemeral than S3).

## Best Practices

- Start debugging from the slowest stage, not from the top-level job duration.
- Read the physical plan when joins, aggregations, or writes are expensive.
- Treat `collect()` as a driver-memory operation, not a distributed operation.
- Track input size, output size, task count, shuffle size, and spill for production jobs.
- Keep transformations composable and inspectable so query plans remain understandable.

## Anti-Patterns

- Explaining Spark only as "parallel Python" or "parallel SQL."
- Tuning executor memory before finding the expensive stage.
- Calling many actions on the same expensive lineage without caching or checkpointing when reuse is intentional.
- Using `collect()` or `toPandas()` on production-size datasets.

## Example

```python
df = spark.read.parquet("s3://lake/events/")

daily = (
    df.filter("event_date = '2026-04-25'")
      .groupBy("customer_id")
      .count()
)

daily.write.mode("overwrite").parquet("s3://lake/customer_daily_counts/")
```

The filter can usually be pipelined with the scan. The `groupBy` requires rows with the same `customer_id` to meet in the same partition, so Spark inserts a shuffle and creates a new stage. The write stage then creates output files from the final partitions.

## Real Use Case

A daily marketing attribution pipeline reads clickstream events, joins them to campaigns, groups by campaign and day, and writes aggregates. When the job slows down, a staff engineer checks the SQL tab and sees the aggregation stage consumes 80 percent of runtime because it shuffles hundreds of GB. The fix is not "add memory" first; the fix is to inspect key distribution, partition sizing, and whether the aggregation can be reduced earlier or written with a better table layout.
