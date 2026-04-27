# Chapter 2 — Shuffle And Performance

Shuffle is the boundary where Spark stops being “embarrassingly parallel” and becomes
a distributed database in the worst sense: network, local disk, memory pressure, and
retries all show up at once. This chapter is the practical companion to
[Chapter 1 — Execution Model](01-execution-model.md): Chapter 1 explains *where* stages
split; this chapter explains *what crosses the split* and how to debug it.

## When This Chapter Matters

- A stage dominates runtime and the Spark UI shows large **Shuffle Read** or **Shuffle
    Write** numbers.
- You are choosing or defending `spark.sql.shuffle.partitions` (or letting AQE choose)
    and need a **testable mental model**.
- You see **spill**, **long-tail tasks**, or **`FetchFailedException`** during or after
    shuffle stages.
- You are designing joins or aggregations over **S3-backed tables on EMR** and need to
    separate shuffle bytes from object-store bytes.

## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming
this page):

- What a shuffle is (map-side write vs reduce-side read) and why it is expensive.
- Which operators typically introduce shuffles and stage boundaries.
- How to debug a slow shuffle stage from the Spark UI (shuffle bytes, spill, skew, long
    tails).
- How to reason about `spark.sql.shuffle.partitions` and how AQE changes the story.
- What a `FetchFailedException` usually means in production.

## Core Idea

A shuffle redistributes data between executors so rows with the same key (or ordering
requirement) arrive at the right downstream tasks. It is required for many joins,
aggregations, `distinct`, window operations, repartitioning, and global sorts.

Shuffle is expensive because it adds **network IO**, **disk IO** (map-side files and
spill), **serialization**, **memory pressure**, and **failure recovery** (lost shuffle
blocks force recomputation).

> **Common mistake:** treating **shuffle** time as “cluster is too small.” Often the
> smallest fix is **less data through the shuffle** (filters, projections, a better
> **join** plan) or **skew** work — not a linear increase in executor count.

![Placeholder: Spark UI stage with high shuffle read/write and spill in summary metrics](../assets/screenshots/placeholder-spark-ui-shuffle-spill.png)

<!-- Screenshot placeholder: `placeholder-spark-ui-shuffle-spill.png` — chapter: shuffle cost. Caption: high shuffle + spill → reduce bytes or fix partitions/skew before adding executors. -->

Caption: **Shuffle**-heavy stages show up as large **Shuffle Read/Write**; **spill** means the in-memory **aggregate/sort** working set did not fit — confirm whether **partition count**, **row width**, or **skew** is the driver before raising memory alone.

## Key Takeaways

- **Shuffle is often the most expensive Spark operation** because it combines network,
    disk, CPU, and memory pressure.
- **`Exchange` in a plan usually means a shuffle boundary**.
- **Shuffle partition count controls reduce-side task size and output shape**.
- **Fetch failures often mean lost shuffle data**, executor loss, disk issues, or
    network instability.

## What Shuffle Is (And What It Is Not)

A shuffle is not “writing to S3” and not “reading the lake.” In the Spark UI,
**shuffle write** on stage *N* means executors wrote **intermediate shuffle files to
local disk** for stage *N+1* to read. **Shuffle read** on stage *N+1* means tasks
**fetched** those bytes over the network (directly or via an external shuffle service,
depending on deployment).

If you confuse shuffle bytes with S3 bytes, you will fix the wrong layer.

## Mental Model

Think of shuffle as a distributed exchange:

1. Map-side tasks partition output into buckets and write shuffle blocks to **executor-
      local** storage.
2. Reduce-side tasks fetch the blocks they own from many upstream tasks.
3. Spark merges, sorts, aggregates, or joins the fetched data.

Every reduce task may need data from every mapper. That all-to-all pattern is why
shuffle is often the dominant cost.

```text
Map tasks                 Shuffle storage                  Reduce tasks
----------                ----------------                ------------
map-0  writes block r0 -> executor disks / network ->     reduce-0 fetches blocks from many maps
map-1  writes block r0 -> executor disks / network ->     reduce-0 merges / sorts / aggregates
map-2  writes block r1 -> executor disks / network ->     reduce-1 fetches its own blocks
```

For a visual walkthrough of the same idea, see
[`diagrams/shuffle-read-write.md`](../../diagrams/shuffle-read-write.md).

| Shuffle cost | Why it matters | Typical symptom |
| --- | --- | --- |
| Network IO | Reduce tasks fetch remote blocks | Long fetch wait, fetch failures |
| Disk IO | Map outputs and spills use local disk | High spill, slow tasks |
| Serialization | Rows cross process or node boundaries | High CPU with shuffle-heavy stages |
| Failure recovery | Lost shuffle files may require recomputation | `FetchFailedException` |

## Shuffle Write And Shuffle Read

**Shuffle write (map side):** each map task hashes (or range-partitions) rows by the
shuffle key and appends to per-partition spill files. Spark eventually materializes
shuffle data on local disk so the next stage can pull it. This is CPU + disk + memory
(before spill).

**Shuffle read (reduce side):** each reduce task requests its partition of the shuffle
from all map outputs, then performs the downstream operator (sort-merge join leg, hash
aggregate finalization, etc.). This is network + merge CPU + memory.

**Production implication:** a single lost executor that still held map-side shuffle
output can invalidate many reduce tasks — each one fails with fetch failure until
Spark recomputes the lost map partitions.

## Common Shuffle-Heavy Operations

| Area | Examples | What to verify first |
| --- | --- | --- |
| Aggregations | `GROUP BY`, `distinct`, multi-stage counts | Partial aggregates before `Exchange` in `EXPLAIN` |
| Joins | Large-large equi-joins | `SortMergeJoin` + two `Exchange` nodes unless broadcast applies |
| Windows | `partitionBy` without compatible layout | Whether an `Exchange` precedes the window |
| Reshaping | `repartition`, `orderBy` | Deliberate vs accidental wide dependencies |

## Why It Matters In Production

Shuffles dominate many production Spark costs. They determine:

- Network traffic between nodes.
- Disk usage for intermediate data.
- Executor memory pressure.
- Stage retry behavior.
- Long-tail task behavior.
- Object-store or local-disk pressure depending on deployment.

Large shuffles also amplify **data skew**. If one key owns most rows, one shuffle
partition becomes a straggler even when averages look fine.

## Production Smells

- A stage has high shuffle read or shuffle write relative to input size.
- Many tasks spill to disk.
- Fetch failures appear during reduce-side shuffle reads.
- One or a few tasks run much longer than the rest.
- CPU is low but the job waits on network or disk.
- Executors are lost during or immediately after large shuffle stages.

## How Shuffle Appears In The Spark UI

| Signal | Where | What it means |
| --- | --- | --- |
| Shuffle write bytes | Upstream stage, Stages tab | Bytes written to local shuffle files |
| Shuffle read bytes | Downstream stage, Stages tab | Bytes fetched into reduce tasks |
| Spill (memory / disk) | Stage metrics | Working set exceeded comfortable memory; often disk-bound afterward |
| Max vs median task duration | Stage summary | Skew, stragglers, or bad partition count |
| `Exchange` | SQL / plan view | Stage boundary and shuffle key |

Always pair **SQL** (where the `Exchange` is) with **Stages** (how expensive that
boundary was).

## Shuffle Partitions

`spark.sql.shuffle.partitions` sets the default number of reduce-side shuffle
partitions for Spark SQL / DataFrame shuffles when AQE does not reshape the plan.
Default `200` is arbitrary relative to your data: it can be too high for small jobs
and too low for terabyte shuffles.

### When increasing shuffle partitions helps

- Reduce tasks are **too large**: high shuffle read per task, heavy spill, executor OOM
    on the reduce side.
- You have **enough cluster cores** to run more tasks without drowning in scheduler
    overhead.
- You need **smaller output files** from a shuffle-heavy write (still validate file
    count afterward).

### When increasing shuffle partitions hurts

- Tasks become **too small**: scheduler delay dominates; Parquet footers and task setup
    dominate.
- Writes explode into **small files** on S3 — each commit and listing cost rises.
- **Skew persists**: more partitions does not split a single hot key; you need a skew
    strategy (see [Chapter 5 — Data Skew](05-data-skew.md) and [Chapter 6 — Adaptive
    Query Execution](06-adaptive-query-execution.md)).

Practical rule: tune until **stage duration, spill, and max/median task ratio**
stabilize, then add a **guardrail metric** (shuffle bytes, spill bytes, max task time)
so regressions alert before the SLA does.

## Skew, Spill, And AQE

**Skew** shows up as max task time far above median with one or a few tasks owning
most shuffle read. The shuffle layer is where skew becomes a wall-clock problem.

**Spill** means Spark kept correctness by pushing data to disk mid-operator. It is
better than an immediate OOM, but it is a signal that per-task working set, partition
count, or operator choice is wrong for the memory budget.

**AQE** (when enabled) can coalesce small shuffle partitions, change join strategy
after seeing sizes, and split skewed partitions. It reduces how precisely you must
guess `spark.sql.shuffle.partitions` up front — it does **not** remove the need to
understand skew or write-side file counts. See [Chapter 6 — Adaptive Query
Execution](06-adaptive-query-execution.md) for limits and validation.

## How To Reduce Shuffle (Design Levers)

- **Filter and project early** so fewer bytes cross the `Exchange`.
- **Broadcast** safely small relations instead of shuffling both sides of a join.
- **Pre-aggregate** on the map side where partial aggregates exist (`count`, `sum`; not
    `collect_list`).
- **Eliminate accidental wide ops** (`orderBy` on huge datasets, unnecessary
    `repartition`).
- **Align storage layout** with join and aggregate keys where the table design allows
    (partitioning, clustering — see [Chapter 3 — Partitioning](03-partitioning.md) and
    [Chapter 23 — Data Modeling And Table Design](23-data-modeling-and-table-
    design.md)).

## EMR, YARN, And S3 Considerations

- **Shuffle files live on executor local disks** (instance store or EBS). Disk
    saturation during shuffle is a real EMR failure mode unrelated to S3 bandwidth.
- **Spot task nodes** can disappear mid-stage. If they held shuffle map output, you pay
    for recomputation and see `FetchFailedException` noise. Long shuffle stages and Spot
    are a deliberate risk tradeoff — see [Chapter 11 — Spark On AWS EMR And
    YARN](11-spark-on-yarn-and-emr.md).
- **S3 is still your source and sink**, not shuffle storage. High shuffle write with low
    S3 output is normal for intermediate stages.
- **Event logs on S3** (`spark.eventLog.dir`) matter for post-mortems when the cluster
    is gone — shuffle forensics often require the UI from the run, not only driver
    stdout.

## Spark SQL Example — Seeing The Exchange

```sql
EXPLAIN FORMATTED
SELECT country, count(*) AS n
FROM sales
WHERE sale_date = '2026-04-25'
GROUP BY country;
```

Look for `Exchange hashpartitioning(country, …)` between partial and final aggregates
— that line is the shuffle boundary Chapter 1 told you to hunt for.

## Common Failure Modes

- `FetchFailedException` after an executor holding shuffle blocks dies.
- `ExecutorLostFailure` due to memory pressure, disk pressure, container kill, or node
    loss.
- Slow reduce tasks caused by skewed shuffle partitions.
- Excessive spill from undersized partitions, bad join strategy, or insufficient memory.
- Too many shuffle partitions causing scheduler overhead and tiny output files.
- Too few shuffle partitions causing large tasks and poor parallelism.

## Tuning And Configuration

`spark.sql.shuffle.partitions` controls the default partition count for SQL/DataFrame
shuffles when AQE does not override it.

Choose shuffle partitions from workload shape:

- Small data: fewer partitions to reduce overhead.
- Large data: enough partitions to keep task input sizes manageable.
- Skewed data: more partitions alone rarely fixes hot keys — pair partition changes with
    skew handling.
- Writes: partition count affects output file count.

With AQE enabled, Spark can coalesce small shuffle partitions after observing runtime
sizes. Validate the **final** plan in the UI; do not assume the initial number is what
ran.

## Spark UI Signals

In the Stages tab, inspect:

- Shuffle read and shuffle write size.
- Spill memory and spill disk.
- Task duration distribution.
- Input size per task.
- GC time.
- Failed tasks and fetch failures.

In the SQL tab, look for `Exchange` nodes. Exchanges usually mark shuffle boundaries.

## Best Practices

- Treat large shuffles as design events, not incidental implementation details.
- Pre-filter data before joins and aggregations.
- Project only needed columns before shuffle-heavy operations.
- Prefer broadcast joins for safely small dimension tables.
- Use AQE, but verify the final adaptive plan.
- Monitor shuffle size and spill as first-class production metrics.

## Anti-Patterns

- Setting `spark.sql.shuffle.partitions` to a universal company-wide number.
- Increasing partitions blindly without checking output files or task overhead.
- Ignoring skew because the average task size looks reasonable.
- Repartitioning repeatedly between transformations without a physical reason.

## Example

```python
orders = spark.read.parquet("s3://lake/orders/")
customers = spark.read.parquet("s3://lake/customers/")

result = (
    orders.join(customers, "customer_id")
          .groupBy("country")
          .sum("order_total")
)
```

This query can have two shuffles: one for the join if `customers` is not broadcast,
and another for the aggregation by `country`. A production review should ask whether
`customers` is small enough to broadcast, whether columns are pruned before the join,
and whether `country` has skew.

## Self-check (concept review)

- What is a Spark shuffle?
- Why is shuffle expensive?
- What files are created during shuffle?
- How does Spark handle shuffle spill to disk?
- What causes `ExecutorLostFailure` during shuffle?
- What is `spark.sql.shuffle.partitions`?
- Why is the default value `200` sometimes bad?
- How do you choose a good number of shuffle partitions?
- What is shuffle fetch failure?
- How do you debug a slow shuffle stage?

## Real Use Case

A fraud pipeline joins 6 TB of transactions with account risk features and then
aggregates by merchant. The SQL tab shows two large exchanges and the Stages tab shows
1.5 TB of shuffle spill. The production fix is to filter transactions to the target
date range before the join, broadcast the small risk-feature table if safe, increase
initial shuffle parallelism for the large aggregation, and validate whether top
merchants create skewed reduce partitions.
