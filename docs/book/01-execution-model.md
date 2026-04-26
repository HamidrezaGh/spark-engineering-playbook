# Chapter 1 — Execution Model

This is the foundation chapter. If you only get one Spark mental model right, get this one right. Almost every production decision — tuning, debugging, design review, cost — is downstream of understanding how Spark turns code into jobs, stages, and tasks, and what runs where.

## When This Chapter Matters

Reach for this chapter when:

- You are tuning or resizing executors without knowing **which stage** is dominant in the Spark UI.
- An incident report says “Spark was slow” and you need to translate that into **jobs, stages, tasks, shuffle, or driver** behavior.
- You are reviewing a design and need to predict **where stage boundaries** will appear before the job runs.
- You are operating on **EMR/YARN** and need to separate “driver died” from “executor lost shuffle” from “YARN killed the container.”

If you already read stages fluently in the UI, skim the **EXPLAIN** section and the EMR notes — those are the production differentiators.

## What You Should Be Able To Answer

After this chapter, you should be able to answer quickly, from memory:

- What is a job, a stage, and a task?
- What creates a new stage, and why?
- What is the difference between a narrow and a wide transformation?
- Why do `groupBy`, `join`, and `distinct` usually cause a shuffle?
- What happens internally when a simple action like `count()` is called?
- How does Spark decide how many tasks a stage will have?
- What is the role of the driver vs the executors?
- What happens if the driver dies mid-job? What if an executor dies mid-job?
- How do these concepts show up in the Spark UI?
- How does this look on AWS EMR / YARN in practice?

If you can't answer any one of these, you'll find the rest of the book harder than it needs to be.

## Core Idea (One Paragraph)

Spark turns a logical computation into distributed work. You write transformations and actions; Spark builds a logical plan, optimizes it, splits it into stages at shuffle boundaries, and runs many tasks across executors. The driver is where planning, scheduling, and result collection happen. The executors are where data is read, computed, and written. The Spark UI is the source of truth for what actually happened.

The useful production model is:

- **Job** — work triggered by one action.
- **Stage** — a set of tasks that can run without crossing a shuffle boundary.
- **Task** — one unit of work over one partition of data.

Shuffles sit *between* stages. The upstream stage performs a **shuffle write** (partitioning + writing shuffle files to local disk). The downstream stage performs a **shuffle read** (fetching those files over the network) and continues processing.

### Spark application vs job vs stage vs task

These terms are easy to conflate because people say “the job” when they mean “the Spark application.” In this book:

| Term | What it is | Production handle |
| --- | --- | --- |
| **Application** | One running Spark driver plus its executors for a single `SparkSession` / context — from `spark-submit` start until exit | YARN application / EMR step; owns all jobs below |
| **Job** | Work triggered by **one action** (`count`, `write`, `collect`, …) | Spark UI **Jobs** tab: one row per action |
| **Stage** | A section of the physical plan that runs without crossing a shuffle boundary | **Stages** tab; count jumps at each `Exchange` |
| **Task** | One partition’s worth of work inside one stage | Stage detail → **Tasks** table |

Multiple actions → multiple jobs in the same application. Each job has its own stage DAG, but they share executors and cached data for that application.

A simple example: `df.groupBy("k").count()` typically becomes:

```text
Stage 0 (shuffle write):  scan -> filter -> partial aggregate -> write shuffle buckets by k
Stage 1 (shuffle read):   read shuffle buckets -> final aggregate -> output partitions
```

## Glossary (Practical, Not Theoretical)

- **Transformation** — builds a plan (lazy). Examples: `select`, `filter`, `withColumn`, `join`, `groupBy`.
- **Action** — triggers execution. Examples: `count`, `collect`, `show`, `write`.
- **Partition** — the unit of parallelism for a dataset; tasks operate on partitions.
- **Narrow dependency** — each output partition depends on a small, bounded number of input partitions. Pipeline-friendly. No shuffle.
- **Wide dependency** — output partitions depend on many input partitions. Requires redistribution. Almost always a shuffle.
- **Shuffle** — distributed redistribution of intermediate data (local disk + network), introducing a stage boundary.
- **Exchange** — the physical-plan node that represents a shuffle. When you see `Exchange` in `EXPLAIN`, you have a stage boundary.

## Narrow vs Wide Transformations

The distinction matters because it tells you whether Spark can pipeline work or has to redistribute data.

| Transformation | Type | Reason |
| --- | --- | --- |
| `select`, `filter`, `withColumn`, `map` | Narrow | Each output partition is computed from one input partition. |
| `union` | Narrow | Concatenation of partitions. |
| `groupBy(...).agg(...)` | Wide | Rows with the same key must end up in the same partition. |
| `join` (without broadcast) | Wide | Both sides must be co-partitioned by the join key. |
| `distinct` | Wide | Equivalent to `groupBy` over all columns. |
| `orderBy` (global sort) | Wide | All rows must agree on a global ordering. |
| `repartition(n)` | Wide | Round-robin or hash redistribution across n partitions. |
| `coalesce(n)` (no shuffle) | Narrow | Merges partitions on existing executors without redistribution. |
| Most window functions over partitioned/ordered windows | Wide | Need rows with the same window key together. |

Wide transformations are the cost centers. They are why `EXPLAIN` matters.

## What Runs Where (Driver vs Executors)

### Driver

- Builds and optimizes the plan. Catalyst's analyzer and optimizer run here.
- Breaks the physical plan into stages and tasks; schedules tasks.
- Tracks metadata, progress, accumulators, and broadcast variables.
- Collects action results (this is what makes `collect()` dangerous).

### Executors

- Run tasks: read input, compute, write output, write shuffle blocks.
- Store cached data and shuffle files on local disk.
- Send heartbeats to the driver. Lost executors are detected here.

### Production rule of thumb

- **Driver memory** is where `collect()`, `toPandas()`, huge query plans, and large file listings hurt.
- **Executor memory, disk, and network** are where shuffle-heavy operators hurt.

These are different failure modes with different fixes. Conflating them is one of the most common debugging mistakes.

## Where Data Is Actually Read And Written

Spark UI uses "read" and "write" generically. In production it helps to separate two very different things:

| Layer | What It Is | Storage |
| --- | --- | --- |
| Source / sink | Your input and output paths | S3 (`s3://`), HDFS (`hdfs://`), local FS (`file:/`) |
| Shuffle | Spark's intermediate data exchange between stages | Executor-local disks; fetched over the network |

Two production implications people get wrong:

- **"Shuffle write is high" does not mean you are writing to S3.** It means you are writing temporary shuffle files to executor disks and pressuring local disk + network.
- **"S3 read time is high" is not the same as "the job is shuffle-bound."** They live in different parts of the plan, and they have different fixes.

If you can't tell whether the bottleneck is in the source/sink layer or the shuffle layer, you can't pick the right fix.

## What Creates A New Stage

The "why did Spark split here?" cheat sheet:

- **`Exchange` in the physical plan** — almost always a shuffle, therefore a stage boundary.
- **Wide transformations** — `groupBy`, most `join`s (unless broadcast), `distinct`, global `orderBy`, `repartition`, most window operations.
- **Writes** — sort, bucket, and commit semantics can introduce extra stages depending on format and connector.

If you're unsure where a stage boundary came from: **Spark UI → SQL tab → click the query → look for `Exchange` nodes → correlate to the Stages tab.**

## Why `groupBy`, `join`, and `distinct` Usually Shuffle

Each of these requires rows that share a key to end up in the same task. Spark has no way to do that without either:

1. Broadcasting one side of the relation to every executor (only feasible if it's small), or
2. Redistributing both sides by the key — a shuffle.

| Operation | Default Strategy | When Shuffle Is Avoided |
| --- | --- | --- |
| `groupBy(...).agg(...)` | Partial aggregate + shuffle + final aggregate | Rare (only with very specific pre-partitioning + bucketing) |
| `join` | Sort-merge join (shuffle both sides) | Broadcast hash join when one side is small |
| `distinct` | Shuffle by all columns | Almost never |
| Window over `partitionBy` | Shuffle by partition key | If already partitioned by the same key |

This is also why partial aggregation matters. For `groupBy(...).count()`, Spark first does a partial count *per input partition* (cheap, no shuffle), then shuffles only the partial results to combine. This is why a `count` aggregation can be enormously cheaper than a `collect_list`-style aggregation: the latter has nothing to pre-aggregate.

## What Happens When `count()` Is Called

Walk through what Spark actually does when you run an action. We'll use a SQL example because that's what most production work looks like.

```sql
SELECT customer_id, count(*) AS n
FROM events
WHERE event_date = '2026-04-25'
GROUP BY customer_id;
```

Here is what happens, end to end:

1. **Parse and analyze.** Catalyst parses the SQL and resolves `events`, `event_date`, and `customer_id` against the catalog (Glue / Hive Metastore on EMR).
2. **Logical optimization.** Catalyst rewrites the logical plan: predicate pushdown, column pruning, constant folding, partial aggregate insertion.
3. **Physical planning.** Catalyst picks a physical plan: which scan node, which aggregate strategy, where to insert `Exchange`.
4. **Stage planning.** The DAG scheduler splits the physical plan into stages at every `Exchange`.
5. **Task scheduling.** For each stage, the scheduler launches one task per partition, respecting locality and resource availability.
6. **Execution.** Executors read shuffle blocks (if any), compute, optionally spill to disk, and write either shuffle output or final output.
7. **Result / commit.** The driver collects results (for `count`, just the row count) or commits the output (for `write`).

For this query, you typically see two stages:

- **Stage 0** — scan `events` with the partition filter `event_date = '2026-04-25'` pushed down, project `customer_id`, do a partial aggregate (`count` per partition per `customer_id`), and write shuffle output partitioned by `customer_id`.
- **Stage 1** — read the shuffled partial counts, do the final aggregate by `customer_id`, return the results.

If you ran `SELECT count(*) FROM ...` instead (no `GROUP BY`), Spark would do a single shuffle to a single partition for the final count. That's why a global `count(*)` over a giant table is sometimes slower than expected: the final reduce is single-task.

## Reading An EXPLAIN — A Worked Example

This is the most useful production skill in this chapter. Let's read a physical plan and identify the stage boundaries, task counts, and skew risk.

```sql
EXPLAIN FORMATTED
SELECT customer_id, count(*) AS n
FROM events
WHERE event_date = '2026-04-25'
GROUP BY customer_id;
```

A typical (simplified) physical plan looks like this:

```text
== Physical Plan ==
* HashAggregate(keys=[customer_id], functions=[count(1)])
+- Exchange hashpartitioning(customer_id, 200)
   +- * HashAggregate(keys=[customer_id], functions=[partial_count(1)])
      +- * ColumnarToRow
         +- FileScan parquet events[customer_id, event_date]
              PartitionFilters: [event_date = 2026-04-25]
              PushedFilters: []
              ReadSchema: struct<customer_id:string>
```

How to read this in production order, top to bottom:

1. **`FileScan parquet events`** — this is the source-side I/O. The `PartitionFilters` line shows partition pruning is happening (good — only one day's partitions will be listed and read). The `ReadSchema` line shows column pruning is happening (only `customer_id` is read, not the full row). If either of these is missing in your real plan, you have an optimization gap.

2. **`HashAggregate(... partial_count(1))`** — partial aggregation. This runs on the map side, before the shuffle. It reduces the number of rows that need to be shuffled, often by orders of magnitude. If you wrote a UDAF or `collect_list` instead of `count`, this step would be much weaker.

3. **`Exchange hashpartitioning(customer_id, 200)`** — **this is the stage boundary.** Spark will redistribute rows by `customer_id` into 200 shuffle partitions. The `200` is `spark.sql.shuffle.partitions` (the default; AQE may coalesce this at runtime).

4. **`HashAggregate(... count(1))`** — final aggregation, running on the reduce side after the shuffle.

What this tells you, immediately:

| Question | Answer From The Plan |
| --- | --- |
| How many stages? | Two — the `Exchange` is the boundary. |
| How many tasks in stage 0? | One per input file split. For a Parquet table, this depends on file count and `spark.sql.files.maxPartitionBytes`. |
| How many tasks in stage 1? | 200 (or fewer, if AQE coalesces). One per shuffle partition. |
| Where is shuffle read/write? | `Exchange` line. Stage 0 writes shuffle. Stage 1 reads shuffle. |
| Where is the skew risk? | The `Exchange hashpartitioning(customer_id, 200)`. If a few `customer_id` values dominate, those partitions become hot. |

This is the loop you run on every non-trivial query: read the plan, find the `Exchange` nodes, count the stages, predict where skew will hit, then go look at the Spark UI to verify.

## How Spark Decides The Number Of Tasks

This is one of the most asked-and-misunderstood Spark questions.

For a **scan stage** (reading files), task count is driven by:

- The number of input file splits.
- `spark.sql.files.maxPartitionBytes` — the target size for one read partition (default 128 MB). Larger files are split, multiple small files are coalesced into one task up to this size.
- `spark.sql.files.openCostInBytes` — used as a small-file penalty to avoid creating excessive tasks for tiny files.

For a **shuffle stage**, task count is driven by:

- `spark.sql.shuffle.partitions` (default 200) — the requested number of shuffle partitions.
- AQE coalescing (if enabled) — Spark may reduce the count at runtime based on observed shuffle sizes.

For a **write stage**, output file count is driven by the final partition count of the stage that feeds the write, plus any partitioning columns specified by `partitionBy`. Wrong partition count here is the most common cause of small-file or huge-file incidents.

The production version of this knowledge:

- "Why so many tasks?" → look at scan partition count and `maxPartitionBytes`.
- "Why so few tasks?" → look at shuffle partition count, broadcast threshold, and whether AQE coalesced too aggressively.

## Failure Semantics

### What if the driver dies?

The Spark application fails. Tasks running on executors are no longer being scheduled or tracked, and the executors will eventually shut down. You must restart the application from the beginning unless your job manages its own checkpointing (Structured Streaming, for example).

In practice, driver death usually comes from:

- `collect()` or `toPandas()` on too much data.
- A query plan that is too large (millions of operators in a generated plan).
- Driver heap pressure from huge file listings (this is a real failure mode on S3-backed jobs that read tens of millions of files).
- A YARN AM container loss (in cluster mode) caused by the underlying node going away.

### What if an executor dies?

Spark will:

1. Mark the executor lost and detect failed tasks.
2. Reschedule those tasks on other executors.
3. If the executor was holding shuffle output, downstream stages that need that shuffle output will throw `FetchFailedException` and Spark will retry — recomputing the lost map output by re-running parts of the upstream stage.

Most jobs survive a small number of executor losses with longer runtime. Frequent executor loss (Spot reclamation, node failures, YARN container kills) usually shows up as cascading retries, repeated stages, and wall-clock time blowing up.

This is why, on EMR, **shuffle-heavy SLA-critical jobs should be careful about running on Spot task nodes**: a single Spot reclamation during the shuffle stage can cost you the whole stage.

## How To Inspect This In The Spark UI

Map every concept above to the UI:

| Concept | Spark UI Surface | What To Look For |
| --- | --- | --- |
| Job | **Jobs** tab | Which action triggered work and how long it took. |
| Stage boundary | **SQL** tab → `Exchange` nodes; **Stages** tab counts stages | One `Exchange` ≈ one new stage. |
| Task count | **Stages** tab → stage detail → "Tasks" | Per-stage task count and per-task metrics. |
| Skew | **Stages** tab → "Summary Metrics" → max vs median task time | A long tail means skew or bad partition sizing. |
| Shuffle read/write | **Stages** tab → "Shuffle Read"/"Shuffle Write" columns | High write on the upstream stage, high read on the downstream stage. |
| Spill | **Stages** tab → spill memory / spill disk | Indicates per-task working set is too large for executor memory. |
| Driver memory pressure | **Executors** tab → driver row | High GC or heap usage on the driver row, often from `collect()` or huge plans. |
| Executor death | **Executors** tab → "Failed Tasks" / "Lost" status | Cascading executor loss often correlates with shuffle pressure or Spot reclamation. |

If you're learning Spark UI for the first time, see [`docs/field-guides/spark-ui-reading-guide.md`](../field-guides/spark-ui-reading-guide.md) for the 90-second triage workflow.

## How This Looks On EMR / YARN

The execution model above is universal; the production shape on EMR has a few specifics worth knowing:

- The driver runs in a YARN container in cluster mode (preferred for production) or on the submit host in client mode (notebooks, gateways). Notebook-driven client-mode driver loss is a very common production incident.
- Executors are YARN containers on EMR core or task nodes. Container memory limits are enforced by YARN, so an executor exceeding its memory budget is killed by YARN — and shows up in YARN logs, not Spark logs.
- Shuffle data is stored on executor-local disk (instance store or attached EBS). When a Spot task node is reclaimed, its shuffle output goes with it.
- Spark event logs should be persisted to S3 (`spark.eventLog.dir=s3://.../spark-event-logs/`) so post-mortem analysis is possible after the cluster terminates. This is non-negotiable for production EMR jobs.

For a deeper treatment, see [Chapter 11 — Spark On AWS EMR And YARN](11-spark-on-yarn-and-emr.md).

## Common Failure Modes

- **Driver OOM** from `collect()`, `toPandas()`, huge query plans, huge file listings, or too much accumulated metadata.
- **Executor loss** from memory pressure, disk pressure, container preemption, node failure, or network instability.
- **Long-tail stages** where one or two tasks run far longer than peers (skew).
- **Excessive task overhead** from too many tiny partitions (scan or post-shuffle).
- **Underutilized cluster** from too few partitions on a large stage.
- **`FetchFailedException`** when shuffle output is lost (executor died, disk full, node gone).

Each of these maps to a different fix, which is why "the job is slow" is never a useful ticket on its own.

## Production Debug Loop

This is the workflow staff engineers actually run during incidents. It is intentionally repetitive because it works.

### 1. Find the bottleneck

- Spark UI → **Jobs**: which action dominates runtime?
- Spark UI → **Stages**: which stage dominates? Compare median vs max task time.
- Spark UI → **SQL**: where are the `Exchange` nodes and what join strategy was chosen?
- Spark UI → **Executors**: are executors dying, GC heavy, or spilling?
- Logs: confirm OOMs, fetch failures, retries, timeouts. On EMR, check YARN container kill reasons.

### 2. Classify the stage

Match the dominant stage to a symptom, then pick the smallest safe first lever (not the largest config change you can make).

| Symptom | Likely cause | Smallest safe fix |
| --- | --- | --- |
| High shuffle read/write | Wide transformation; check filter/projection placement | Push filter/projection earlier; reduce shuffle volume |
| Max task time ≫ median | Skew | AQE skew handling, salt the hot key, or pre-aggregate |
| High spill | Per-task working set too large | More partitions; better join strategy; remove unnecessary cache |
| High input read time/bytes | Scan-bound | Partition pruning; column pruning; small-files compaction |
| Many tiny tasks + scheduler delay | Over-partitioned | Coalesce or reduce shuffle partitions |
| Lots of failed/retried tasks | Instability | Fix executor loss, disk, or network before tuning SQL |

### 3. Change one thing

Re-run. Compare the same stage metrics: duration, max task time, shuffle bytes, spill, input bytes.

If you change three things at once, you have not learned anything; you have only changed the wall-clock time. Single-variable change is non-negotiable for real diagnostic work.

## Best Practices

- Start every diagnosis from the slowest stage, not from the top-level job duration.
- Read the physical plan whenever joins, aggregations, or writes look expensive.
- Treat `collect()` and `toPandas()` as driver-memory operations, not distributed operations.
- Track input bytes, output bytes, task count, shuffle bytes, and spill as production metrics for every job.
- Keep transformations composable so the physical plan stays understandable.
- Prefer Spark SQL when it expresses the work clearly. Catalyst optimizes SQL much more aggressively than user-defined Python loops.

## Anti-Patterns

- Explaining Spark only as "parallel Python" or "parallel SQL" — this hides every interesting failure mode.
- Tuning executor memory before identifying the expensive stage.
- Using `collect()` or `toPandas()` on production-size datasets.
- Calling many actions on the same expensive lineage without caching or checkpointing when reuse is intentional.
- Setting `spark.sql.shuffle.partitions` to a single company-wide value and assuming it works for every job.

## Worked Example — End To End

A daily marketing attribution pipeline reads clickstream events, joins them to campaigns, groups by campaign and day, and writes aggregates.

```sql
SELECT
  c.campaign_id,
  e.event_date,
  count(*)  AS clicks,
  sum(e.revenue) AS revenue
FROM events e
JOIN campaigns c
  ON e.campaign_id = c.campaign_id
WHERE e.event_date = '2026-04-25'
GROUP BY c.campaign_id, e.event_date;
```

If the job slows from 25 minutes to 2 hours, here's the staff-level loop:

1. Open the **SQL** tab. Confirm there are two `Exchange` nodes (one for the join, one for the aggregation) — or only one if `campaigns` is small enough to broadcast.
2. Open the **Stages** tab. Find the stage backing the larger `Exchange`. Look at task duration distribution.
3. If max task time is far above the median: skew. Investigate `campaign_id` distribution. Likely fix: salting or AQE skew join handling.
4. If task durations are uniform but the stage is enormous: shuffle volume. Check whether columns and filters are pushed down. Likely fix: prune `events` more aggressively, ensure `campaigns` is broadcastable, validate AQE coalesce.
5. If executors are dying: instability. Look at YARN container kill reasons before changing any Spark config.

The goal is never "add memory." The goal is to identify which of the failure modes above is happening, and apply the smallest fix that resolves it.

## Real Use Case

A nightly EMR job processed clickstream and joined it to a campaign dimension table. The job was stable at 25 minutes for a year and then quietly became 2 hours. Nothing in the code had changed.

A staff engineer opened the Spark UI:

- The slow stage was the aggregation, not the join.
- Max task time in that stage was 40× median — clear long tail.
- A quick query against the clickstream showed one new `campaign_id` had been launched and accounted for ~35% of all rows for that day.

The fix was not to add memory. The fix was AQE skew join handling plus a `top-1 key concentration` guardrail metric on the source table, so the next time skew shifts, the alert fires before the SLA does. The slow stage went back to ~30 minutes; the guardrail caught a similar shift two weeks later.

That's the loop. Mental model → physical plan → Spark UI → smallest fix → guardrail. Everything else in this book is a refinement of this.
