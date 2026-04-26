# Joins


## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- What join strategy Spark chose (broadcast vs sort-merge vs shuffled hash) and why.
- When a broadcast join is safe vs a memory-risk anti-pattern.
- What “two big tables join” implies operationally (shuffle size, skew risk, spill risk).
- How to read the plan (`explain("formatted")` / SQL tab) to see `Exchange` + join operators.
- What the first practical fixes are (filter/project early, stats, key cleanup, skew handling).

## Core Idea

Join strategy determines how Spark brings matching rows together. For production workloads, the strategy can be the difference between a fast local hash lookup and a multi-terabyte shuffle.

The main strategies are broadcast hash join, sort-merge join, shuffled hash join, broadcast nested loop join, and cartesian-style joins for special cases.

## Key Takeaways

- **Broadcast joins avoid shuffling the large side** when the small side safely fits in executor memory.
- **Sort-merge joins are common for large equi-joins** because they scale beyond memory.
- **Join key skew can dominate runtime** even when average input size looks normal.
- **Always filter and project before large joins**.

## Mental Model

Spark must satisfy the join condition. If one side is small enough, Spark can broadcast it to every executor and avoid shuffling the large side. If both sides are large, Spark usually shuffles both sides by join key so matching keys land in the same partitions.

Join performance depends on input size, join key distribution, column pruning, filters, statistics, memory, and table layout.

```text
Join inputs
  |
  |-- Is one side safely small?
  |      -> yes: broadcast hash join
  |              check broadcast size and executor memory
  |
  |-- Are both sides large equi-joins?
  |      -> yes: sort-merge join
  |              check shuffle size and skew
  |      -> sometimes: shuffled hash join
  |              check per-partition build-side memory
  |
  |-- Non-equi or unusual condition
         -> other strategies, often expensive
```

| Strategy | Best Fit | Main Risk |
| --- | --- | --- |
| Broadcast hash join | Huge table joined to small table | Broadcast side too large for memory |
| Sort-merge join | Two large equi-join inputs | Large shuffle and sort cost |
| Shuffled hash join | Per-partition build side is small | Memory pressure |
| Broadcast nested loop | Non-equi or cross-style cases | Explosive runtime and output |

## What Spark Does Internally

Broadcast hash join sends the smaller build side to executors. Each task scans its partition of the large side and probes an in-memory hash relation.

Sort-merge join shuffles both sides by join key, sorts each side within partitions, and merges sorted streams. It is commonly chosen for large equi-joins because it scales predictably and handles data larger than memory through external sorting.

Shuffled hash join shuffles both sides and builds a hash table for one side per partition. It can be faster than sort-merge when the per-partition build side is small enough, but it is more sensitive to memory pressure.

## Why It Matters In Production

Bad join choices cause huge shuffles, executor OOM, skew, unexpected cartesian products, and large output explosions. A staff-level review always asks:

- Is one side safely broadcastable?
- Are filters and projections pushed before the join?
- Are join keys clean and correctly typed?
- Is the join cardinality expected?
- Are statistics accurate enough for the optimizer?
- Is there key skew?

## Production Smells

- Spark picks a sort-merge join when one side should be broadcastable.
- Broadcast joins fail with executor memory pressure.
- Join output is much larger than expected.
- A join stage has severe task skew.
- The physical plan contains nested loop joins unexpectedly.

## Common Failure Modes

- Broadcast side exceeds executor or driver memory.
- Missing stats prevent Spark from choosing broadcast.
- Duplicate keys create multiplicative output growth.
- Null-heavy or dirty join keys reduce match quality.
- Large joins spill because partitions are too large.
- Skewed keys create long-tail reduce tasks.

## Tuning And Configuration

`spark.sql.autoBroadcastJoinThreshold` controls the size threshold Spark uses for automatic broadcast joins. Broadcast hints can override optimizer choices, but they should be used when the engineer understands memory risk.

For huge-to-small joins:

- Filter and project the small side.
- Broadcast it if it is safely small.
- Validate broadcast size in the physical plan.

For huge-to-huge joins:

- Reduce both sides before the join.
- Ensure compatible join key types.
- Handle skew explicitly.
- Consider table layout, bucketing, clustering, or partition pruning when applicable.
- Validate output cardinality.

## Spark UI Signals

Use `explain("formatted")` and the SQL tab to identify:

- `BroadcastHashJoin`.
- `SortMergeJoin`.
- `ShuffledHashJoin`.
- `BroadcastNestedLoopJoin`.
- `Exchange` nodes before joins.
- Runtime row counts and shuffle sizes.

In the Stages tab, inspect shuffle read, spill, and task skew for join stages.

## Best Practices

- Select only needed columns before joining.
- Filter as early as possible.
- Prefer broadcast joins for genuinely small dimensions.
- Treat join key skew as a first-class design issue.
- Check join cardinality with profiling queries.
- Keep table statistics current where the optimizer relies on them.

## Anti-Patterns

- Broadcasting a table because it is "dimension-like" without checking actual size.
- Joining on strings with inconsistent normalization.
- Ignoring duplicate keys on the build side.
- Using hints to hide missing statistics without understanding the plan.
- Joining two huge tables before applying available filters.

## Example

```python
from pyspark.sql.functions import broadcast

orders = spark.read.parquet("s3://lake/orders/")
countries = spark.read.parquet("s3://lake/dim_country/").select("country_id", "country_name")

enriched = orders.join(broadcast(countries), "country_id", "left")
```

Broadcasting `countries` avoids shuffling the large `orders` table. This is good only if `countries` is small enough to fit safely on every executor.

## Interview-Style Questions Covered

- Explain the different join strategies in Spark.
- What is a broadcast hash join?
- When does Spark choose broadcast join?
- What is `spark.sql.autoBroadcastJoinThreshold`?
- What happens if the broadcast table is too large?
- What is a sort-merge join?
- What is a shuffled hash join?
- Why does Spark often use sort-merge join for large tables?
- How do you optimize a join between a huge table and a small table?
- How do you optimize a join between two huge tables?

## Real Use Case

A customer 360 pipeline joins 3 TB of transactions to a 20 MB country dimension and a 600 GB customer profile table. The country join should broadcast. The customer join is a large join that needs filters, column pruning, key profiling, and skew checks. Treating both joins the same would either waste shuffle or risk broadcast memory failures.
