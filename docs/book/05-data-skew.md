# Chapter 5 — Data Skew

Skew is the production failure mode that makes "more cluster" a useless answer. A job with 2,000 tasks where 1,999 tasks finish in 30 seconds and one task runs for 50 minutes is not slow because the cluster is too small. It is slow because the work is unevenly distributed. Adding executors does nothing for the one task that is doing all the work.

This chapter is about diagnosing skew with evidence, applying the smallest fix that resolves it, and adding a guardrail so the same skew shape does not surprise the next on-call engineer.

## What You Should Be Able To Answer

After this chapter, you should be able to answer quickly, from memory:

- How does skew show up in the Spark UI? What metrics tell you it is skew and not "input was bigger today"?
- What is the difference between hot-key skew and hot-partition skew? Which one does AQE help with?
- When does AQE skew join handling activate, and when is it useless?
- What are the two correct ways to salt a hot key — and the one common incorrect way?
- Why is "increase shuffle partitions" rarely the fix for a single hot key?
- How do you isolate a celebrity key without breaking downstream joins?

## What Skew Is — And What It Is Not

Skew is uneven work distribution across tasks. Specifically:

- One or a few tasks process much more data than the rest.
- The job runs as long as the slowest task, no matter how fast the others are.
- The cluster sits mostly idle while the long-tail tasks finish.

Skew is not "the data is large." Large data with even distribution scales horizontally — twice the data takes twice the time on twice the cluster. Skew does not. Twice the cluster does nothing for a single hot task.

The four production patterns of skew:

| Pattern | Where It Happens | Smallest Fix |
| --- | --- | --- |
| Hot-key join skew | Sort-merge join on a key with a few dominant values | AQE skew join, salting, or hot-key isolation |
| Aggregation skew | `GROUP BY` on a key with hot values | Two-phase aggregation with salting |
| File / split skew | One source file is much larger than the rest | Repartition before processing; fix upstream sizing |
| Write skew | Writing partitioned by a column with hot values | Repartition before write; review partition column choice |

Skew differs from spill: spill says "this task's working set is too large for memory," which can come from skew but also from oversized partitions. Skew differs from scheduler delay: scheduler delay says "the driver was slow to dispatch tasks," which is a configuration or driver pressure problem. Diagnose what you have before you fix it.

## Why Skew Is Worse Than "Large Data"

Three production reasons skew is the most expensive performance problem:

1. **It cannot be fixed by adding cluster.** Doubling executors makes 1,999 fast tasks finish faster. The one slow task is unchanged. Cost goes up, runtime does not.
2. **It hides until a key turns hot.** A pipeline can run for nine months with mild skew, then a new merchant launches a viral product and one merchant accounts for 35% of the rows. The plan, the code, and the cluster all look the same. Only the data shape changed.
3. **It causes cascading failures.** A skewed task often spills more, runs longer, and on a Spot or preemptible node, has a higher chance of being reclaimed mid-task. The retry runs the same skewed work over again. Wall-clock blows up.

A staff-level review treats skew as a *data* problem, not a *cluster* problem. The fix is on the partitioning expression, the join key, or the upstream data, not on `spark.executor.memory`.

## Mental Model — Hot Keys And Long-Tail Tasks

During any wide transformation, Spark assigns rows to target partitions based on a partitioning expression — almost always a hash of the key. If one key value appears 50 million times and other key values appear once, all 50 million rows for that hot key land in one shuffle partition. One task processes them. That is the long tail.

A simplified picture:

```text
Even distribution after shuffle (no skew):
partition-0: ###########
partition-1: ##########
partition-2: ###########
partition-3: ##########

Skewed distribution (one hot key):
partition-0: ##########
partition-1: ##############################################################
partition-2: ###########
partition-3: ##########

The cluster waits for partition-1 while partitions 0, 2, 3 idle.
```

The hot partition runs in one task on one executor core. AQE skew join handling can split this partition for some join shapes, but not for arbitrary aggregations.

## Detecting Skew In The Spark UI

The fastest skew diagnosis is the Stages tab. Open the slow stage and look at the Summary Metrics table.

| Metric | What To Compare | What It Tells You |
| --- | --- | --- |
| Duration | Max vs median | A 5×–10× ratio is mild skew; 50×+ is a serious hot key |
| Shuffle Read Size | Max vs median | If max is 10×+ median, one task is fetching far more data |
| Spill (Memory) | Max value across tasks | Concentrated spill on a few tasks confirms the working set is too large for those tasks |
| Spill (Disk) | Max value across tasks | Same as above, but worse — disk spill is slow |
| Input Size | Max vs median | One large input file or split |
| Records | Max vs median | Direct hot-key signature in shuffle stages |

The Spark UI also lets you sort tasks by Duration (descending). The handful of slowest tasks are exactly the ones causing the long tail. Open one and read its stage operators in the SQL tab. You will see whether the work is `SortMergeJoin`, `HashAggregate`, or a write.

The SQL tab visualization shows AQE annotations. If `isSkewedJoin=true` appears on a join operator, AQE detected and handled the skew at runtime. That is not a cure — it is a diagnostic. AQE handled it; verify the long tail actually shortened.

## Detecting Skew With SQL

Before tuning, profile the data. The most useful query is the top-key concentration check:

```sql
SELECT
    customer_id,
    COUNT(*) AS row_count
FROM events
WHERE event_date = DATE '2026-04-25'
GROUP BY customer_id
ORDER BY row_count DESC
LIMIT 20;
```

What to read from the result:

- If the top key is 1–5% of total rows, expect mild skew and let AQE handle it.
- If the top key is 10–35% of total rows, expect significant skew. AQE alone is usually not enough.
- If the top key is 50%+, you have a celebrity-key problem. Plan for hot-key isolation, not generic tuning.

Two follow-up checks worth running before changing any code:

```sql
-- How concentrated is the top 1%?
WITH counts AS (
    SELECT customer_id, COUNT(*) AS row_count
    FROM events
    WHERE event_date = DATE '2026-04-25'
    GROUP BY customer_id
),
ranked AS (
    SELECT
        row_count,
        SUM(row_count) OVER () AS total_rows,
        PERCENT_RANK() OVER (ORDER BY row_count) AS pr
    FROM counts
)
SELECT
    SUM(CASE WHEN pr >= 0.99 THEN row_count ELSE 0 END) AS top_1_pct_rows,
    MAX(total_rows) AS total_rows,
    SUM(CASE WHEN pr >= 0.99 THEN row_count ELSE 0 END) * 1.0 / MAX(total_rows) AS top_1_pct_ratio
FROM ranked;

-- Are nulls or sentinel values dominating?
SELECT
    COALESCE(customer_id, '__NULL__') AS customer_id,
    COUNT(*) AS row_count
FROM events
WHERE event_date = DATE '2026-04-25'
GROUP BY COALESCE(customer_id, '__NULL__')
ORDER BY row_count DESC
LIMIT 5;
```

Null and sentinel values (`""`, `"0"`, `"unknown"`, `null`) are the most common source of an unexpected hot key. They are also the most common source of incorrect joins, because nulls do not match nulls in equi-join semantics. Either filter them, replace with a deterministic sentinel, or document the contract.

## Detecting Skew With PySpark

For ad-hoc investigation, a small PySpark script gives the same answer as the SQL above and is easier to chain into a notebook.

```python
from pyspark.sql import functions as F

events = spark.read.parquet("s3://lake/events/").where("event_date = DATE '2026-04-25'")

top_keys = (
    events
    .groupBy("customer_id")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.desc("row_count"))
    .limit(20)
)
top_keys.show(truncate=False)

total = events.count()
top_share = top_keys.agg(F.sum("row_count")).collect()[0][0] / total
print(f"Top 20 keys = {top_share:.1%} of {total:,} rows")
```

If "top 20 keys" is 30%+ of total rows, expect skew on any wide transformation that uses `customer_id`.

A more diagnostic version computes the per-task partition size after the shuffle:

```python
from pyspark.sql import functions as F

events = spark.read.parquet("s3://lake/events/").where("event_date = DATE '2026-04-25'")
shuffled = events.repartition(200, "customer_id")

shuffled = shuffled.withColumn("__pid", F.spark_partition_id())
sizes = shuffled.groupBy("__pid").count().orderBy(F.desc("count"))
sizes.show(20, truncate=False)
```

A normal output has counts within 2x of each other. A skewed output has one or two partitions with 10–100x more rows. That is the same partition that will be the long-tail task.

## AQE Skew Join — When It Helps

AQE's skew join feature splits a single skewed shuffle partition into multiple smaller subtasks for `SortMergeJoin`. It does this *after* the first shuffle exposes the actual partition sizes. The relevant configs:

- `spark.sql.adaptive.enabled=true`
- `spark.sql.adaptive.skewJoin.enabled=true`
- `spark.sql.adaptive.skewJoin.skewedPartitionFactor` — multiplier above the median that defines "skewed" (default 5).
- `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` — minimum bytes a partition must exceed to be considered skewed (default 256 MB).

When AQE skew join helps:

- `SortMergeJoin` (the most common large-large case).
- The skewed partition is large enough to cross both thresholds.
- The other side has the corresponding partition available so the split task has both inputs.
- Inner, left outer, and right outer joins (Spark's coverage has expanded over versions; verify on your runtime).

What you see when it works:

- The SQL tab shows `isSkewedJoin=true` on the join operator.
- The Stages tab shows the join stage with more tasks than `spark.sql.shuffle.partitions` (because the skewed partition was split).
- Max task time vs median is much closer than it would have been.

The signal that AQE is working is "max task time is no longer 50× the median." If AQE thinks it handled skew but the long tail is still there, AQE made a partial fix; you need additional work.

## AQE Skew Join — When It Does Not Help

AQE skew join is not a universal fix. Cases where it does nothing or only partially helps:

- **Aggregation skew.** AQE splits skewed *join* partitions, not skewed aggregation partitions. If a `GROUP BY` has a hot key, AQE will not split it. You need two-phase aggregation.
- **Single key dominance beyond split benefit.** AQE splits a partition into pieces. If the entire partition is one key value with 50 million rows, you can split into 10 pieces, but each piece is still 5 million rows for the same key. For inner equi-joins, that helps because the build side is replicated. For other operations, it does not help.
- **Custom UDFs that require all rows for a key.** If your logic is "compute a session window per user" and one user has 5 million events, splitting does not help — the session computation needs all of them together.
- **Skew in non-`SortMergeJoin` plans.** A `BroadcastHashJoin` with skew on the probe side cannot be skew-joined; the build side is already on every executor, but the probe side's hot partition is still one task.
- **Plan does not match thresholds.** The skewed partition is below the byte threshold, or the factor is below the multiplier. Default thresholds were chosen to avoid over-triggering. On medium clusters, AQE may not consider 100 MB skewed.

If the SQL tab does not show `isSkewedJoin=true` and you expected it to, check the thresholds and the join shape. Do not assume AQE silently activated.

## Manual Salting Pattern

Salting splits a hot key across multiple synthetic keys, processes each in parallel, and then re-aggregates back to the original key. It works for both joins and aggregations.

The simplest correct salting recipe for an aggregation:

```sql
-- Step 1: salt rows. Each row gets a random salt in [0, N).
WITH salted AS (
    SELECT
        customer_id,
        FLOOR(RAND() * 16) AS salt,
        amount
    FROM events
    WHERE event_date = DATE '2026-04-25'
),
-- Step 2: aggregate by (customer_id, salt). 16x more keys -> hot key splits.
phase1 AS (
    SELECT
        customer_id,
        salt,
        SUM(amount) AS partial_amount
    FROM salted
    GROUP BY customer_id, salt
)
-- Step 3: aggregate back to the original key.
SELECT
    customer_id,
    SUM(partial_amount) AS total_amount
FROM phase1
GROUP BY customer_id;
```

What changes in execution:

- The first `GROUP BY (customer_id, salt)` shuffles by a 16x larger key space. The hot `customer_id` is now spread across 16 partitions.
- The second `GROUP BY customer_id` is small — its input is the partial aggregates, not the full data. This stage has fewer rows to process, so the hot key is no longer a problem.

For a join, salting is more involved because both sides need salts that match. The pattern:

```sql
-- Salt the hot side.
WITH salted_orders AS (
    SELECT
        order_id,
        customer_id,
        FLOOR(RAND() * 16) AS salt,
        amount
    FROM orders
    WHERE order_date = DATE '2026-04-25'
),
-- Replicate the cold side across all salts.
exploded_customers AS (
    SELECT
        customer_id,
        customer_segment,
        salt
    FROM customers
    LATERAL VIEW EXPLODE(SEQUENCE(0, 15)) s AS salt
)
SELECT
    o.order_id,
    o.amount,
    c.customer_segment
FROM salted_orders o
JOIN exploded_customers c
    ON o.customer_id = c.customer_id
   AND o.salt = c.salt;
```

The cold side is replicated 16 times, once per salt value. Memory cost: 16x on the cold side. That is the tradeoff. Use this pattern only when:

- The cold side is small enough that 16x replication is still affordable.
- The hot side cannot be reduced first (semi-join, pre-aggregate).
- AQE skew join did not resolve the long tail.

If the cold side is large, replicating 16x is worse than the original skew. Pick a different tactic: hot-key isolation.

## Two-Phase Aggregation — The Correct Form

Two-phase aggregation is the same pattern as salted aggregation, but for any aggregation where the partial-final composition is associative. Sum, count, min, max, and approx_count_distinct work. Median, distinct_count, and arbitrary UDAFs may not.

The minimum pattern:

```python
from pyspark.sql import functions as F

events = spark.read.parquet("s3://lake/events/").where("event_date = DATE '2026-04-25'")

salt_buckets = 32

# Phase 1: aggregate by (customer_id, salt).
phase1 = (
    events
    .withColumn("__salt", (F.rand() * salt_buckets).cast("int"))
    .groupBy("customer_id", "__salt")
    .agg(F.sum("amount").alias("partial_amount"))
)

# Phase 2: aggregate back to customer_id.
result = (
    phase1
    .groupBy("customer_id")
    .agg(F.sum("partial_amount").alias("total_amount"))
)
```

What this gives you:

- Phase 1 is the expensive shuffle. It now spreads each hot `customer_id` across `salt_buckets` partitions, breaking the long tail.
- Phase 2 is cheap. The intermediate cardinality is at most `distinct customers × salt_buckets`, which is much smaller than the input.

The most common bug in this pattern is forgetting Phase 2. The output of Phase 1 is partial sums by `(customer_id, salt)`, not by `customer_id`. If you stop after Phase 1 and write the result, you have an incorrect output that looks plausible until someone reconciles totals.

## Incorrect Salting — A Common Bug

The bug is to salt the input, run the aggregation, and forget to combine back. Or, worse, to salt for an aggregation that is not associative.

```python
from pyspark.sql import functions as F

# WRONG: salting an arbitrary UDAF.
events = spark.read.parquet("s3://lake/events/")
salted = events.withColumn("__salt", (F.rand() * 16).cast("int"))

# This computes "median per (customer_id, salt)", not "median per customer_id".
# Median is not associative: median(median(a), median(b)) != median(a + b).
result = (
    salted
    .groupBy("customer_id", "__salt")
    .agg(F.expr("percentile_approx(amount, 0.5)").alias("median_amount"))
)
```

For non-associative aggregations, salting changes the result. Use a different approach: isolate the hot key, compute its true median in a separate stage, and union with the rest. Or use approximate algorithms that are decomposable, such as t-digest, when accuracy permits.

The general rule: if `agg(agg(a), agg(b)) == agg(a + b)`, salting is safe. If not, salting is a correctness bug.

## Isolating Hot Keys

For celebrity-key cases — one or two keys that account for 30%+ of rows — isolation is often cleaner than salting. The pattern:

```sql
WITH hot_keys AS (
    SELECT customer_id
    FROM (VALUES ('cust_42'), ('cust_117')) AS t(customer_id)
),
hot_orders AS (
    SELECT *
    FROM orders
    WHERE order_date = DATE '2026-04-25'
      AND customer_id IN (SELECT customer_id FROM hot_keys)
),
cold_orders AS (
    SELECT *
    FROM orders
    WHERE order_date = DATE '2026-04-25'
      AND customer_id NOT IN (SELECT customer_id FROM hot_keys)
)
-- Hot path: process with bespoke logic, possibly broadcast or salted.
-- Cold path: process with the standard plan.
SELECT * FROM (
    SELECT /*+ BROADCAST(c) */ ho.*, c.customer_segment
    FROM hot_orders ho
    JOIN customers c ON ho.customer_id = c.customer_id
)
UNION ALL
SELECT * FROM (
    SELECT co.*, c.customer_segment
    FROM cold_orders co
    JOIN customers c ON co.customer_id = c.customer_id
);
```

When isolation is the right tool:

- The set of hot keys is small (single-digit) and stable enough that maintaining the list is reasonable.
- The hot keys correspond to a real business meaning — a flagship enterprise customer, a default tenant — that may want different processing.
- Pure salting blows up memory because the cold side is large.
- Operational visibility per hot key is valuable: the on-call engineer wants to know how the flagship customer's run went, separately.

When isolation is the wrong tool:

- The hot key set changes daily.
- Maintenance overhead exceeds the runtime savings.
- The hot keys are bots or noise; you should be filtering them, not specially handling them.

## Repartitioning vs Coalescing

A common skew "fix" is `df.repartition(N)` or `df.coalesce(N)` before a stage. Understand what each does before reaching for it.

| Operation | What It Does | When It Helps Skew |
| --- | --- | --- |
| `repartition(n)` | Full shuffle, redistributes by random hash | Helps when input file/split skew is the problem; does nothing for hot-key skew on a downstream `groupBy` |
| `repartition(n, col)` | Full shuffle, redistributes by hash of `col` | If `col` has hot keys, you reproduce the skew. Same partitioning expression, same problem |
| `coalesce(n)` | No shuffle, merges partitions on the same executor | Only useful for reducing partition count without a shuffle (e.g., before write); makes skew worse if you coalesce skewed partitions |
| `repartitionByRange(n, col)` | Range-partition by samples | Better for sort or write-by-range; not a skew fix on its own |

Specifically, `repartition(N)` does *not* fix hot-key skew. The hash is on the partition expression. Repartitioning by `customer_id` produces the same partition for the hot customer regardless of `N`. Repartitioning by random hash spreads rows out, but the next `groupBy customer_id` shuffles them right back together — and the hot key reassembles in one partition.

The only repartitioning that helps skew is repartitioning by a *different* expression that happens to balance work — for example, `repartition(200, customer_id, hash_bucket)` if `hash_bucket` is computed to break up the hot key. That is just salting in a different syntax.

## Why "Just Add More Executors" Is Often The Wrong Fix

When skew is the problem, more executors does not help because:

- The cluster has more cores than tasks already; you are bottlenecked on one task.
- Larger executors mean more memory per task, which can make a single hot task succeed where it was OOMing — but you have not addressed the underlying long tail.
- Cost goes up linearly with executor count. Runtime is unchanged.

Add executors only when the diagnosis says "every task is slow because the per-task working set is too large and we cannot reduce it." That is rarely the same diagnosis as "max task is 50× the median."

The recommended diagnostic order:

1. Confirm skew with the Stages tab (max vs median).
2. Identify the hot key with a profiling query.
3. Pick the smallest fix: AQE skew join, salting, isolation, semi-join.
4. Add a guardrail: a metric that fires when the top key concentration exceeds a threshold.
5. Add executors only after steps 1–4 have not resolved the runtime problem.

## Worked Example — Celebrity Customer Key

Workload: a daily subscription analytics job aggregates events by `account_id`. One enterprise account produces 35% of all events for the day. The aggregation stage takes 50 minutes, with 1,999 tasks finishing in under 3 minutes and one task running the full 50.

Diagnosis with SQL first:

```sql
SELECT account_id, COUNT(*) AS rows
FROM events
WHERE event_date = DATE '2026-04-25'
GROUP BY account_id
ORDER BY rows DESC
LIMIT 5;

-- account_id    | rows
-- acct_flagship | 87,431,200
-- acct_b        | 4,210,300
-- acct_c        | 3,983,100
-- acct_d        | 3,540,000
-- acct_e        | 3,001,200
```

The flagship account is 20× the next account and ~35% of the day. AQE skew join does not apply (this is an aggregation, not a join). Two-phase aggregation is the right tool.

Better version:

```python
from pyspark.sql import functions as F

events = spark.read.parquet("s3://lake/events/").where("event_date = DATE '2026-04-25'")

salt_buckets = 64  # bigger than usual because the flagship is 20x the next account

phase1 = (
    events
    .withColumn("__salt", (F.rand() * salt_buckets).cast("int"))
    .groupBy("account_id", "__salt")
    .agg(F.sum("amount").alias("partial_amount"))
)

result = (
    phase1
    .groupBy("account_id")
    .agg(F.sum("partial_amount").alias("total_amount"))
)
```

Result after the change:

- Phase 1 stage runtime: 5 minutes (was 50). Max-to-median task ratio dropped from ~80× to ~3×.
- Phase 2 stage runtime: 30 seconds. The intermediate row count is `distinct accounts × 64`, which is small.
- End-to-end runtime: 6 minutes (was 50).

Guardrail added: a daily metric for top-1 account share of total rows. If it exceeds 25%, the on-call engineer is alerted that skew may be increasing. The same alert fires before the SLA does, two weeks later, when a new viral product launches.

## Bad Fix vs Better Fix

| Problem | Bad Fix | Better Fix |
| --- | --- | --- |
| One hot key in a join | Add more executors | AQE skew join; if not enough, salt or isolate |
| One hot key in an aggregation | Increase shuffle partitions | Two-phase aggregation with explicit salt |
| Null/sentinel hot key | Ignore it | Filter or normalize at ingest; document the contract |
| Skew in `repartition(n)` output | Try a different `n` | Repartition is not the fix; address the partitioning expression |
| Long tail on a write | Coalesce to 1 partition | Repartition by a balanced expression; review partition column |
| AQE skew join did not trigger | Disable AQE | Check thresholds; lower `skewedPartitionThresholdInBytes` if the workload is small |
| Salting changed result correctness | Live with it | Audit aggregation associativity; use isolation for non-associative agg |
| Hot key changes daily | Maintain a hardcoded list | Compute hot keys at runtime via a percentile threshold |
| OOM only on hot tasks | Increase executor memory | Identify which task and which key; salt or split the work |
| Long tail on Spot | Move all work to on-demand | Move only the long-tail stage to on-demand; keep the rest on Spot |

## Production Smells

- A stage where max task time is 10× the median, and the on-call's first instinct is to add memory.
- An aggregation that has been stable for months and suddenly takes 5x longer; the data shape changed, not the code.
- A new "hot key" that turns out to be a default value (`""`, `"unknown"`, `"0"`) the upstream pipeline just started emitting.
- A salting implementation with no second aggregation, producing partial results that look right.
- AQE enabled but `isSkewedJoin=true` never appears, even on obviously skewed jobs — thresholds are too high.
- A skew "fix" that involves caching the hot side; caching does not redistribute work.
- A pipeline that runs on Spot and has frequent long-tail tasks; reclamation probability on long tasks is high.

## Spark UI Signals

Quick signals when you open the slow stage:

- **Summary Metrics → Duration → Max vs Median.** 5×–10× is mild. 50×+ is a hot key.
- **Summary Metrics → Shuffle Read Size.** Concentrated read size on the slow tasks confirms a key-based long tail.
- **Summary Metrics → Spill (Memory) and Spill (Disk).** Concentrated spill says the per-task working set is too large for memory.
- **Tasks tab → sort by Duration descending.** The slowest 5 tasks are your incident.
- **SQL tab → join operator.** `isSkewedJoin=true` means AQE detected and split. If you expected this to fire and it did not, check thresholds.

For a stage with a clear long tail, the quickest diagnostic is one click: SQL tab → click the query → click the operator that feeds the slow stage → look for `numPartitions`, `numOutputRows` per partition, and the AQE annotations. Spark prints enough metadata for the diagnosis to be unambiguous.

## Staff-Level Review Checklist

Before approving a Spark change that involves a wide transformation on a key with any business meaning:

- [ ] The top-key concentration on the join or aggregation key is known and below a threshold (commonly 10%).
- [ ] Null and sentinel values on the key are intentional and documented.
- [ ] AQE is enabled and the skew thresholds are tuned for the typical partition size in this workload.
- [ ] The job emits a per-run metric for top-key concentration, max-to-median task duration, and total shuffle bytes on the heaviest stage.
- [ ] If salting is used, the second aggregation back to the original key is present and tested.
- [ ] If salting is used on a non-associative aggregation, the change is reviewed against correctness.
- [ ] Hot-key isolation, if present, has a documented business reason and a clear hot-key list source.
- [ ] The job does not run skewed shuffle stages on Spot capacity.
- [ ] An alert fires when the top-key concentration exceeds the threshold, before the SLA is at risk.
- [ ] The runbook includes the SQL queries needed to identify the hot key during an incident.

## Anti-Patterns

- Treating skew as a tuning problem and reaching for executor memory or shuffle partitions first.
- Salting an aggregation without verifying it is associative.
- Forgetting Phase 2 of two-phase aggregation; producing partials that look like results.
- Repartitioning by the same column you were going to group by; you do not change the hash.
- Using AQE as a black-box "fix"; assuming `isSkewedJoin=true` means the long tail is gone, without verifying max-to-median.
- Caching one side of a skewed join; caching does not change the partition mapping.
- Running long-tail shuffle stages on Spot. Reclamation probability is non-trivial on tasks that run for tens of minutes.
- Hardcoding hot-key lists that drift; the pipeline ends up under-isolating the new hot key and over-isolating last quarter's hot key.
- Adding executors and declaring victory because the job finished. Cost went up, runtime did not.

## Real Use Case

A B2B subscription platform runs a daily analytics aggregation grouping events by `account_id`. The job had been stable for nine months. After a flagship enterprise customer's rollout, the job slowed from 25 minutes to 90 minutes, then started failing with executor OOMs once a week.

The on-call team raised `spark.executor.memory` twice. The OOMs decreased but runtime stayed at 90 minutes.

A staff engineer ran the top-key query and saw the flagship account at 35% of rows. The fix was three pieces:

1. Two-phase aggregation with `salt_buckets = 64` for the heaviest aggregation stage.
2. A `top-1 key share` daily metric on the source table, with an alert at 25%.
3. A runbook entry pointing to the diagnostic SQL and this chapter.

Runtime returned to ~25 minutes. The metric fired again two months later when a different flagship customer's rollout started; the on-call engineer ran the same playbook and resolved it before the SLA was at risk.

The lesson is not the salting recipe. The lesson is that skew is a *data shape* problem and the platform should expose data shape as a first-class metric. A team that monitors top-key concentration on every important key catches skew before it catches them.
