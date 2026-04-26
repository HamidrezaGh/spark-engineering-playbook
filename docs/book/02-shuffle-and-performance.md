# Shuffle And Performance

Status: First Draft
Level: Senior to Staff
Covers: shuffle mechanics, shuffle files, spill, fetch failures, shuffle partition sizing

## Core Idea

A shuffle redistributes data between executors so rows with the same key or ordering requirement arrive at the right downstream tasks. It is required for many joins, aggregations, `distinct`, window operations, repartitioning, and global sorts.

Shuffle is expensive because it adds network IO, disk IO, serialization, sorting, memory pressure, and failure recovery complexity.

## Mental Model

Think of shuffle as a distributed exchange:

1. Map-side tasks write shuffle blocks partitioned by reduce partition.
2. Reduce-side tasks fetch their blocks from many executors.
3. Spark merges, sorts, aggregates, or joins the fetched data.

Every reduce task may need data from every mapper. This all-to-all pattern is why shuffle is often the most expensive part of a Spark job.

```text
Map tasks                 Shuffle storage                 Reduce tasks
----------                ---------------                 ------------
map-0  writes block r0 -> executor disks/network ->       reduce-0 fetches blocks from many maps
map-1  writes block r0 -> executor disks/network ->       reduce-0 merges/sorts/aggregates
map-2  writes block r1 -> executor disks/network ->       reduce-1 fetches its own blocks
```

| Shuffle Cost | Why It Matters | Symptom |
| --- | --- | --- |
| Network IO | Reduce tasks fetch remote blocks | Long fetch wait, fetch failures |
| Disk IO | Map outputs and spills use local disk | High spill, slow tasks |
| Serialization | Rows cross process/node boundaries | High CPU with shuffle-heavy stages |
| Failure recovery | Lost shuffle files may require recomputation | `FetchFailedException` |

## What Spark Does Internally

During shuffle, Spark writes intermediate data to local disks on executors. The exact file layout depends on Spark version and shuffle manager, but the practical artifacts are data files, index metadata, and block metadata that let reduce tasks fetch the correct ranges.

If shuffle data does not fit in memory during sort, aggregate, or join processing, Spark spills to disk. Spill is a pressure-release mechanism: it prevents immediate OOM, but it usually means extra disk IO and slower tasks.

Shuffle fetch failures occur when reduce tasks cannot fetch shuffle blocks. Common causes include lost executors, deleted shuffle files, network issues, disk failure, node preemption, or external shuffle service problems.

## Why It Matters In Production

Shuffles dominate many production Spark costs. They determine:

- Network traffic between nodes.
- Disk usage for intermediate data.
- Executor memory pressure.
- Stage retry behavior.
- Long-tail task behavior.
- Object-store or local-disk pressure depending on deployment.

Large shuffles also amplify data skew. If one key owns a large fraction of data, one reduce partition may become much larger than the rest.

## Production Smells

- A stage has high shuffle read or shuffle write relative to input size.
- Many tasks spill to disk.
- Fetch failures appear during reduce-side shuffle reads.
- One or a few tasks run much longer than the rest.
- CPU is low but the job waits on network or disk.
- Executors are lost during or immediately after large shuffle stages.

## Common Failure Modes

- `FetchFailedException` after an executor holding shuffle blocks dies.
- `ExecutorLostFailure` due to memory pressure, disk pressure, container kill, or node loss.
- Slow reduce tasks caused by skewed shuffle partitions.
- Excessive spill from undersized partitions, bad join strategy, or insufficient memory.
- Too many shuffle partitions causing scheduler overhead and tiny output files.
- Too few shuffle partitions causing large tasks and poor parallelism.

## Tuning And Configuration

`spark.sql.shuffle.partitions` controls the default number of shuffle partitions for Spark SQL and DataFrame workloads when AQE does not override it. The default value `200` can be too high for small jobs and too low for large jobs.

Choose shuffle partitions from workload shape:

- Small data: fewer partitions to reduce overhead.
- Large data: enough partitions to keep task input sizes manageable.
- Skewed data: more partitions may help only if the skewed key can be split or AQE can split skewed partitions.
- Writes: partition count affects output file count.

With AQE enabled, Spark can coalesce small shuffle partitions after seeing runtime sizes. This reduces the need to perfectly tune the initial number, but it does not remove the need to understand skew and output file sizing.

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

This query can have two shuffles: one for the join if `customers` is not broadcast, and another for the aggregation by `country`. A production review should ask whether `customers` is small enough to broadcast, whether columns are pruned before the join, and whether `country` has skew.

## Interview-Style Questions Covered

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

A fraud pipeline joins 6 TB of transactions with account risk features and then aggregates by merchant. The SQL tab shows two large exchanges and the Stages tab shows 1.5 TB of shuffle spill. The production fix is to filter transactions to the target date range before the join, broadcast the small risk-feature table if safe, increase initial shuffle parallelism for the large aggregation, and validate whether top merchants create skewed reduce partitions.
