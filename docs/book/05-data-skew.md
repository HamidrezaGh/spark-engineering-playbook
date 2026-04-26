# Data Skew


## Core Idea

Data skew happens when work is unevenly distributed across partitions. In Spark, skew usually means a small number of tasks process much more data than the rest, often because a few keys dominate the dataset.

Skew is a distribution problem, not simply a "cluster too small" problem.

## Key Takeaways

- **Skew is visible as long-tail tasks**, not just large total data size.
- **A single hot key can keep one task running while the cluster waits**.
- **AQE helps with some skewed joins**, but it is not a universal fix.
- **Salting can split hot keys**, but it adds correctness and maintenance complexity.

## Mental Model

During a key-based shuffle, Spark assigns rows to target partitions based on a partitioning expression, commonly a hash of the key. If one key appears millions of times, all rows for that key may land in the same partition. That one partition becomes a long-running task.

This is the celebrity key problem:

```python
customer_id = "123"  # millions of rows
```

Even if the average partition size looks fine, one partition can dominate runtime.

```text
Normal keys after shuffle:
partition-0: #######
partition-1: ######
partition-2: ########
partition-3: #######

Skewed celebrity key after shuffle:
partition-0: ######
partition-1: ##################################################
partition-2: #####
partition-3: #######
```

| Fix | When It Helps | Tradeoff |
| --- | --- | --- |
| AQE skew join | Moderate skew in supported join plans | Not universal and threshold-dependent |
| Broadcast join | One side is safely small | Memory risk if size is underestimated |
| Salting | One or a few keys dominate | Extra complexity and second aggregation |
| Isolate hot keys | Celebrity keys have special business meaning | More pipeline branches to operate |

## What Spark Does Internally

Skew appears after exchanges such as joins, aggregations, distinct operations, and window functions. Spark schedules one task per resulting partition. If one partition contains far more rows or bytes, Spark cannot automatically split that logical key in all cases.

AQE can detect skewed shuffle partitions for certain joins and split them into smaller pieces. This helps when the skew is visible at runtime and the operation is supported. It does not solve every skew pattern, especially custom logic, extreme single-key aggregation, or cases where correctness requires all rows for a key to be processed together.

## Why It Matters In Production

Skew creates long-tail stages. Most tasks finish quickly, then the cluster waits for one or a few slow tasks. This wastes compute, increases SLA risk, and often causes memory spill or executor loss.

Skew is especially dangerous in:

- Joins on customer, tenant, merchant, product, or account IDs.
- Aggregations by low-cardinality or uneven keys.
- Event streams with bot traffic or hot entities.
- Multi-tenant platforms where a few tenants are much larger than the rest.

## Production Smells

- One task runs for tens of minutes while sibling tasks finish quickly.
- Shuffle read size is concentrated in a small number of tasks.
- A few keys dominate the row count.
- AQE helps but does not eliminate long-tail tasks.
- Executor OOM happens only on specific reduce tasks.

## Common Failure Modes

- Slow stage caused by one hot key.
- OOM during skewed join or aggregation.
- Spill-heavy reduce task.
- Salting creates incorrect results because the second aggregation is missing.
- AQE skew join does not activate because the plan or thresholds do not match.

## Tuning And Configuration

`spark.sql.adaptive.skewJoin.enabled` enables AQE skew join optimization when AQE is enabled. Related AQE thresholds control what Spark considers skewed.

Manual techniques include:

- Filter or isolate hot keys.
- Broadcast the smaller side of a join when safe.
- Salt hot keys to split them across partitions.
- Use two-phase aggregation: aggregate salted keys first, then aggregate back to the original key.
- Process celebrity keys separately with custom logic.
- Increase shuffle partitions only when partition size is generally too large; this alone does not split a single hot key.

## Spark UI Signals

Look for:

- Max task duration much higher than median.
- Max shuffle read much higher than median.
- High spill on a few tasks.
- One reduce task keeping a stage active.
- AQE final plan showing skew partition handling.

Data profiling signals:

```sql
SELECT customer_id, COUNT(*) AS rows
FROM events
GROUP BY customer_id
ORDER BY rows DESC
LIMIT 20;
```

This finds hot keys before they become runtime incidents.

## Best Practices

- Profile key distribution before large joins and aggregations.
- Track max-to-median task duration for shuffle stages.
- Use AQE, but do not depend on it as the only skew strategy.
- Separate known hot keys when they have different business meaning.
- Validate salted logic carefully to avoid changing result semantics.

## Anti-Patterns

- Solving every skew problem by adding executors.
- Increasing shuffle partitions and assuming a hot key will split.
- Salting without a deterministic salt strategy when reproducibility matters.
- Forgetting the second aggregation after salted aggregation.
- Ignoring null or default keys such as `"unknown"` or `0`.

## Example

```python
from pyspark.sql.functions import col, concat_ws, pmod, xxhash64

salted_events = events.withColumn(
    "salted_customer_id",
    concat_ws("#", col("customer_id"), pmod(xxhash64("event_id"), 16))
)
```

This spreads rows for a hot customer across 16 salted keys. For aggregations, you must aggregate by salted key first and then aggregate back to `customer_id`.

## Interview-Style Questions Covered

- What is data skew?
- How do you detect skew from the Spark UI?
- How do you detect skew from data profiling?
- Why does one task run much longer than others?
- How can salting fix skew?
- What are the downsides of salting?
- How does AQE handle skewed joins?
- What is `spark.sql.adaptive.skewJoin.enabled`?
- When is AQE not enough to fix skew?
- How would you handle a celebrity key problem?

## Real Use Case

A subscription analytics job groups events by `account_id`. One enterprise account produces 35 percent of all events, so one reduce task runs for 50 minutes while the rest finish in 3 minutes. The production fix is to profile top accounts, process the celebrity account through a salted two-phase aggregation, keep AQE enabled for moderate skew, and add a data quality metric that alerts when the top key exceeds a threshold.
