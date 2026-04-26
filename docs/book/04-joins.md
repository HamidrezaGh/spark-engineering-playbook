# Chapter 4 — Joins

Joins are where most production Spark performance problems live. Most "Spark is slow" tickets, on inspection, are a join strategy regression, a join key quality problem, a missing pruning step before the join, or skew on the join key. Get this chapter right and you will diagnose 60% of production incidents faster.

This chapter is opinionated and plan-driven. Every recommendation is tied to something you can confirm in `EXPLAIN FORMATTED` or the Spark UI SQL tab. If you cannot confirm it from a plan or UI metric, treat the recommendation as a guess.

## What You Should Be Able To Answer

After this chapter, you should be able to answer quickly, from memory:

- What join strategy did Spark pick, and what evidence in the plan tells you that?
- When is a broadcast join safe, and when is it a memory-risk anti-pattern?
- What does "two large tables join" imply about shuffle, sort, spill, and skew risk?
- Which filters and projections must happen *before* the join for the plan to be acceptable?
- What does AQE change about join strategy at runtime, and what does it not change?
- How do you tell from the SQL tab that the join is the bottleneck, and not the scan or the write?
- What are the three or four production smells that tell you the join is the wrong shape?

## Join Strategies — The Production Set

Spark SQL has five join strategies that matter in practice:

| Strategy | Identifier In Plan | Best Fit | Main Risk |
| --- | --- | --- | --- |
| Broadcast hash join | `BroadcastHashJoin` | Huge fact table joined to a small dimension that fits in executor memory | Build side too large, driver/executor OOM, broadcast timeout |
| Sort-merge join | `SortMergeJoin` | Two large equi-join inputs | Large shuffle, large sort, spill, and key skew |
| Shuffled hash join | `ShuffledHashJoin` | Per-partition build side fits in memory and is much smaller than probe side | Per-task hash table OOM under skew |
| Broadcast nested loop join | `BroadcastNestedLoopJoin` | Non-equi joins where one side is small | Cartesian-style explosion if you are wrong about size |
| Cartesian product | `CartesianProduct` | Tiny inputs only; almost never intentional | Output size explodes; usually a bug |

Two non-strategy join types you also have to recognize:

- `BroadcastExchange` — the physical operator that broadcasts the build side. It precedes a `BroadcastHashJoin` or `BroadcastNestedLoopJoin`.
- `Exchange hashpartitioning(<keys>, <n>)` — the physical operator that shuffles each side by the join keys. It precedes a `SortMergeJoin` or `ShuffledHashJoin`.

If you can name the strategy and the exchange shape from a plan in 10 seconds, the rest of this chapter will feel obvious.

## Mental Model — How Spark Decides

Spark picks a join strategy based on:

1. **Whether the join is equi or non-equi.** Non-equi joins almost always become broadcast nested loop. The optimizer cannot use a hash join when the condition is not equality on a key.
2. **Estimated size of each side.** Stats from the catalog plus runtime hints determine whether either side fits under `spark.sql.autoBroadcastJoinThreshold` (default 10 MB).
3. **Hints in the SQL.** `BROADCAST`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`. Hints override the optimizer when feasible.
4. **AQE runtime evidence.** With AQE on, after the first stage Spark sees real shuffle sizes and may rewrite a planned `SortMergeJoin` into a `BroadcastHashJoin` if the actual size now fits.
5. **AQE skew handling.** AQE may split skewed shuffle partitions for a `SortMergeJoin` into smaller subtasks (`isSkewedJoin`).

Production rule: the optimizer picks reasonably *if* it has accurate statistics and the predicates push down. Most "Spark picked the wrong join" incidents are really "stats were missing or stale" or "a filter that should be on the small side ran after the join."

## What Makes A Join Expensive

Joins are expensive for one of five reasons. Diagnose which one before you tune.

| Cost Driver | Where It Shows Up | Smallest Fix |
| --- | --- | --- |
| Shuffle volume | `Exchange` shuffle write/read bytes | Filter and project both sides before the join |
| Sort cost | `Sort` operator inside `SortMergeJoin`, high CPU and spill | Fewer rows into the join; rethink shuffle partitions |
| Per-task working set | Spill memory/disk on join stage | More shuffle partitions, or a different strategy |
| Skew | Max task time ≫ median in the join stage | AQE skew join, salting, or hot-key isolation |
| Output cardinality | Write stage runtime, output row count | Check for duplicate keys on the build side |

Almost every other "join optimization" reduces to one of these.

## Join Strategy Selection — Decision Tree

```text
Is the join condition an equi-join (k1 = k2)?
  no  -> almost certainly BroadcastNestedLoopJoin or CartesianProduct
         confirm one side is genuinely small or you have a bug

  yes -> Is one side reliably smaller than autoBroadcastJoinThreshold?
           yes -> BroadcastHashJoin (validate broadcast side actually fits in memory)
           no  -> Are both sides large but the per-partition build side fits memory?
                    yes -> ShuffledHashJoin (rare; usually only with hint)
                    no  -> SortMergeJoin (the normal large-large case)

In all cases:
  - Push every filter and projection through the join.
  - Confirm partition pruning fires on partitioned source tables.
  - Confirm key types match exactly. A type cast on a join key disables hash partitioning.
```

## Broadcast Threshold And Hints

`spark.sql.autoBroadcastJoinThreshold` is the size, in bytes, under which Spark will auto-broadcast the smaller side. Default 10 MB. Common production values: `10485760` (10 MB, default), `52428800` (50 MB), `104857600` (100 MB).

Set this knob deliberately. Pushing it to 1 GB across a shared platform is how you manufacture broadcast-induced executor OOMs. Per-job is fine; cluster default is dangerous.

The hints in Spark SQL:

```sql
SELECT /*+ BROADCAST(c) */ o.order_id, c.country_name
FROM orders o
JOIN dim_country c
  ON o.country_id = c.country_id;
```

| Hint | Forces | When To Use |
| --- | --- | --- |
| `BROADCAST(t)` | Broadcast hash join with `t` as build side | Small dimension where stats are missing or wrong |
| `MERGE(t)` | Sort-merge join | Disabling a broadcast that the optimizer wrongly chose |
| `SHUFFLE_HASH(t)` | Shuffled hash join with `t` as build side | Edge cases; rarely the right answer |
| `SHUFFLE_REPLICATE_NL(t)` | Broadcast nested loop with replication | Niche non-equi joins; almost never |

Hints are a contract. If you broadcast a 50 MB dimension today, you are also promising the next person that the dimension stays under that size. Add a row-count or byte-size guardrail when you broadcast a dimension that has any chance of growing.

## AQE — What It Changes For Joins

With AQE on (`spark.sql.adaptive.enabled=true`, default `true` in modern Spark):

- `spark.sql.adaptive.autoBroadcastJoinThreshold` — runtime threshold AQE uses to convert `SortMergeJoin` to `BroadcastHashJoin` after the first shuffle.
- `spark.sql.adaptive.coalescePartitions.enabled` — coalesces small post-shuffle partitions on the join's reduce side, which usually reduces task count and output file count.
- `spark.sql.adaptive.skewJoin.enabled` — splits skewed shuffle partitions for `SortMergeJoin` into multiple smaller tasks.
- `spark.sql.adaptive.skewJoin.skewedPartitionFactor` and `skewedPartitionThresholdInBytes` — control what AQE considers skewed.

What AQE does *not* do:

- It does not rewrite a non-equi join to anything cheaper.
- It does not handle skew when correctness requires all rows for one key in one task (e.g., custom aggregations that cannot be split).
- It does not push filters through the join after the fact. If a filter is on the wrong side of the join in the logical plan, AQE will not move it.

If the SQL tab shows `AdaptiveSparkPlan isFinalPlan=true` over a join and the actual operator is `BroadcastHashJoin` while the static plan said `SortMergeJoin`, AQE made the conversion. That is a good thing; trust it.

## Join Key Quality

Join key quality is more important than any tuning knob. The four production-grade questions to ask of every join:

1. **Are the types exactly equal?** `int = bigint` or `string = int` causes Spark to insert a cast. A cast can disable hash-based partitioning and forces a less efficient plan. Hash of `customer_id::string` is not equal to hash of `customer_id::bigint`.
2. **Are the keys nullable, and do nulls have a business meaning?** Nulls do not equal nulls in SQL. Equi-joins drop them. If your "join" produces fewer rows than expected, check null counts on the keys before tuning anything else.
3. **Are the keys clean?** `"  US"` and `"US"` and `"us"` are three different join keys. Normalize before the join.
4. **Are there duplicate keys on the build side?** A many-to-many join multiplies output rows. If `customers` has two rows for `customer_id = 42` and `orders` has 1,000, the join produces 2,000 rows for that key. Duplicate-key explosions are the most common output cardinality bug.

A staff-level review checks key quality before strategy.

## Partition Pruning And Column Pruning Before Joins

The single most impactful optimization for a slow join is moving filters and projections before it.

Bad — filter and projection happen after a huge join:

```sql
SELECT *
FROM orders o
JOIN customers c
  ON o.customer_id = c.customer_id
WHERE o.order_date = DATE '2026-04-25'
  AND c.country = 'US';
```

The `WHERE` clauses in this query reach the executor as filters that *should* be pushed down. Whether they actually are depends on the source format (Parquet/Iceberg yes; some sources no), the table layout, and whether the predicates reference the partition columns.

Better — explicit pre-join filters and column lists, especially for non-trivial pipelines:

```sql
WITH o AS (
  SELECT order_id, customer_id, order_amount
  FROM orders
  WHERE order_date = DATE '2026-04-25'
),
c AS (
  SELECT customer_id, customer_tier
  FROM customers
  WHERE country = 'US'
)
SELECT o.order_id, o.order_amount, c.customer_tier
FROM o
JOIN c USING (customer_id);
```

Both queries can produce the same plan, but the second form makes intent explicit and makes regressions visible: if a future change removes a column or filter, the diff is obvious.

For partitioned source tables, always confirm `PartitionFilters: [<partition_col> = ...]` appears in the `FileScan` line of the plan. If it does not, you are reading the entire table and the join cost is the smaller problem.

## How To Read Join Operators In `EXPLAIN FORMATTED`

The minimum viable mental model: every join in the physical plan is one operator with two inputs. Each input is either a `BroadcastExchange` (broadcast strategies) or an `Exchange hashpartitioning` (shuffle strategies) or a direct subtree (when the input is already partitioned correctly).

A typical `BroadcastHashJoin`:

```text
* BroadcastHashJoin [customer_id#10], [customer_id#42], Inner, BuildRight
  :- * Project [order_id#7, customer_id#10]
  :  +- * Filter isnotnull(customer_id#10)
  :     +- * ColumnarToRow
  :        +- FileScan parquet orders[order_id#7, customer_id#10, order_date#9]
  :             PartitionFilters: [order_date#9 = 2026-04-25]
  :             PushedFilters: [IsNotNull(customer_id)]
  +- BroadcastExchange HashedRelationBroadcastMode(...), [id=#88]
     +- * Project [customer_id#42, customer_tier#45]
        +- * Filter isnotnull(customer_id#42)
           +- * ColumnarToRow
              +- FileScan parquet customers[customer_id#42, customer_tier#45]
```

Read this top-down:

- `BroadcastHashJoin ... BuildRight` — Spark chose to broadcast the right input.
- The right subtree starts with `BroadcastExchange`. That is the broadcast operator. Its memory cost is your concern.
- The left subtree is a normal scan over the large table. There is no `Exchange hashpartitioning` on this side, which is the entire point of broadcast — no shuffle on the large side.
- `PartitionFilters: [order_date#9 = 2026-04-25]` — partition pruning fired. Without this, you would be scanning the whole table.
- `PushedFilters: [IsNotNull(customer_id)]` — null filter pushed down to Parquet. Equi-join keys are implicitly non-null on Spark's side; pushdown drops null rows at the source.

A typical `SortMergeJoin`:

```text
* SortMergeJoin [customer_id#10], [customer_id#42], Inner
  :- * Sort [customer_id#10 ASC NULLS FIRST], false, 0
  :  +- Exchange hashpartitioning(customer_id#10, 200), ENSURE_REQUIREMENTS, [id=#101]
  :     +- * Project [order_id#7, customer_id#10]
  :        +- * Filter isnotnull(customer_id#10)
  :           +- * ColumnarToRow
  :              +- FileScan parquet orders[...]
  +- * Sort [customer_id#42 ASC NULLS FIRST], false, 0
     +- Exchange hashpartitioning(customer_id#42, 200), ENSURE_REQUIREMENTS, [id=#102]
        +- * Project [customer_id#42, customer_tier#45]
           +- * Filter isnotnull(customer_id#42)
              +- * ColumnarToRow
                 +- FileScan parquet customers[...]
```

Read this top-down:

- `SortMergeJoin` — both sides will be shuffled and sorted.
- Each subtree contains an `Exchange hashpartitioning(customer_id#x, 200)`. Each side is a separate stage. The `200` is `spark.sql.shuffle.partitions` (AQE may coalesce at runtime).
- Each subtree contains a `Sort` step. Sort cost is real. On large inputs, a significant fraction of CPU is in `Sort`, not in the join itself.
- If both subtrees ended with `Exchange hashpartitioning(customer_id#x, <same n>)`, you may also see `ShuffledHashJoin` if Spark decides the build side is small enough per partition to skip the sort.

When AQE intervenes you will see an outer `AdaptiveSparkPlan` wrapper:

```text
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   * BroadcastHashJoin [customer_id#10], [customer_id#42], Inner, BuildRight
     ...
+- == Initial Plan ==
   * SortMergeJoin [customer_id#10], [customer_id#42], Inner
     ...
```

`Initial Plan` was `SortMergeJoin`. `Final Plan` (after the first shuffle exposed the actual size) was `BroadcastHashJoin`. AQE rewrote it. Trust the final plan; that is what ran.

## Spark UI Signals For Join Problems

The SQL tab visualization is the single most useful diagnostic surface for joins. Click the SQL query, then look at the operator graph.

| Symptom | UI Signal | Likely Cause |
| --- | --- | --- |
| Slow join with high shuffle | Both sides have large `Exchange` write and read bytes | Filters not pushed, or both sides genuinely large |
| Slow join with one big task | Stage detail shows max task time ≫ median in the join stage | Skew on the join key |
| Join stage spills | Stage detail shows non-zero spill memory and spill disk on tasks | Per-task working set too large; consider more partitions or skew handling |
| Broadcast looks chosen but driver/executor OOMs | Driver heap or executor failed task right after `BroadcastExchange` | Broadcast side larger than the threshold suggested |
| Output much bigger than expected | Final write stage row count ≫ input row counts | Duplicate keys on the build side; many-to-many join |
| `BroadcastNestedLoopJoin` in the plan | Plan shows `BroadcastNestedLoopJoin` or `CartesianProduct` | Non-equi or missing join condition; almost always a bug |

For shuffle stages specifically, the four metrics worth comparing:

- Shuffle write bytes — how much each map side wrote to local disk.
- Shuffle read bytes — how much each reduce side fetched.
- Spill (memory) and spill (disk) — per-task working set was too large.
- Max task duration vs median — long tail = skew.

## Worked Example 1 — Huge Fact Table + Small Dimension

Workload: 3 TB `orders` joined with a 20 MB `dim_country` dimension. We want country name on each order for a single day.

```sql
EXPLAIN FORMATTED
SELECT
    o.order_id,
    o.order_amount,
    c.country_name
FROM orders o
JOIN dim_country c
    ON o.country_id = c.country_id
WHERE o.order_date = DATE '2026-04-25';
```

What Spark is likely to do:

- `dim_country` at 20 MB is well under the default 10 MB threshold... actually it is over, by 2x. With default settings Spark will not auto-broadcast.
- If you do nothing, you get `SortMergeJoin` with two 1.5 TB shuffles (or worse). Wall-clock time goes from 5 minutes to 30+ minutes for no good reason.
- The fix is either to raise the threshold for this job (`spark.sql.autoBroadcastJoinThreshold=104857600`) or to add `/*+ BROADCAST(c) */`.

What to look for in the physical plan:

- `BroadcastHashJoin ... BuildRight` and `BroadcastExchange` over the `dim_country` scan.
- The `orders` subtree has no `Exchange hashpartitioning`. That is the win.
- `PartitionFilters: [order_date = 2026-04-25]` on the `orders` `FileScan`. Without this, the broadcast is useless because you are scanning the whole fact table.

What to look for in the Spark UI:

- The join stage should be small in shuffle bytes — broadcast does not produce a shuffle on the probe side.
- The driver's `BroadcastExchange` task time is short (seconds). If it is minutes, the dimension is bigger than you think.
- No spill on join tasks; the hash table is small.

Production risk:

- Dimension grows. A 20 MB dimension today becomes 200 MB next year and silently OOMs the executor that builds the hash relation.
- Add a guardrail: emit `dim_country` row count and byte size after every refresh; alert if it exceeds the broadcast threshold.

Better version, with explicit hint and projection:

```sql
SELECT /*+ BROADCAST(c) */
    o.order_id,
    o.order_amount,
    c.country_name
FROM orders o
JOIN (
    SELECT country_id, country_name
    FROM dim_country
) c
    ON o.country_id = c.country_id
WHERE o.order_date = DATE '2026-04-25';
```

Two improvements: the hint forces the broadcast even if stats lie, and the inline subquery makes the broadcast payload columns explicit. If `dim_country` later gains a 1 KB JSON column, the broadcast does not silently inflate.

## Worked Example 2 — Two Large Tables Join

Workload: 3 TB `orders` joined with 600 GB `customer_profile` to enrich orders with customer attributes for a single day.

```sql
EXPLAIN FORMATTED
SELECT
    o.order_id,
    o.order_amount,
    p.customer_segment,
    p.lifetime_value
FROM orders o
JOIN customer_profile p
    ON o.customer_id = p.customer_id
WHERE o.order_date = DATE '2026-04-25';
```

What Spark is likely to do:

- 600 GB cannot be broadcast. The plan will be `SortMergeJoin`.
- Both sides shuffle by `customer_id`. The shuffle volume on the orders side is large because the date filter narrows orders, but the customer-profile shuffle is the entire 600 GB unless we filter it.
- AQE will probably help with skew on `customer_id` if there is any.

What to look for in the physical plan:

- `SortMergeJoin` at the top.
- Two `Exchange hashpartitioning(customer_id, <n>)` children. `n` is `spark.sql.shuffle.partitions`. AQE may coalesce on the reduce side.
- `Sort` operators on both children. The sort cost is a real fraction of runtime.
- `PartitionFilters: [order_date = 2026-04-25]` on `orders`. If missing, fix that first; everything else is downstream noise.

What to look for in the Spark UI:

- Both shuffle stages should report large write bytes. Read bytes on the join stage roughly equal write bytes from the parents.
- Max task time vs median in the join stage. A 5–10× ratio is normal for slightly skewed workloads. 50× means a serious hot key.
- Spill (memory) and spill (disk) on the join stage. Non-zero spill on every task means shuffle partition count is too low; non-zero spill on a few tasks means skew.

Production risk:

- The `customer_profile` shuffle is 600 GB every time. Filter the profile to only the customers that appear in today's orders, if your platform supports a semi-join or a pre-join filter. A semi-join saves shuffle, sort, and spill for the columns you do not need.
- Skew on `customer_id` (a single enterprise account, a bot account, a default placeholder customer id, or null) creates long-tail tasks. AQE skew join helps, but verify with the SQL tab that AQE actually triggered.

Better version, with a semi-join to narrow the profile side:

```sql
WITH active_customers AS (
    SELECT DISTINCT customer_id
    FROM orders
    WHERE order_date = DATE '2026-04-25'
      AND customer_id IS NOT NULL
)
SELECT
    o.order_id,
    o.order_amount,
    p.customer_segment,
    p.lifetime_value
FROM orders o
JOIN customer_profile p
    ON o.customer_id = p.customer_id
JOIN active_customers a
    ON p.customer_id = a.customer_id
WHERE o.order_date = DATE '2026-04-25';
```

The third join with `active_customers` is an inner join that effectively narrows `customer_profile` to only the customers we care about. The actual semi-join shape (`LEFT SEMI JOIN`) is cleaner; some platforms optimize it more aggressively. Either way, the win is shuffle volume — instead of 600 GB on the profile side, you might shuffle 60 GB.

## Worked Example 3 — Join Strategy Regression

Workload: a job that has run for nine months at 8 minutes is now taking 90 minutes. Same code. Same cluster. Same input volumes within a few percent.

The investigation order:

1. Open the SQL tab for the slow run. Find the join. Note the strategy.
2. Open the SQL tab for a known-good run (you persisted event logs to S3, right?). Compare the strategy of the same join.
3. If the strategies differ, the question is *why*. Most likely answers, ranked by frequency:
   - Statistics on one side became stale (a recent ingest changed the row count estimate).
   - Someone bumped `spark.sql.autoBroadcastJoinThreshold` for an unrelated workload, then a refactor narrowed the small side just enough to hit the new threshold elsewhere.
   - A schema change widened the small side past the threshold.
   - AQE was disabled on this cluster after a config rollout.
4. If the strategies are the same but the join is slower, the question is shuffle or skew.

Specific signature: the static plan in the new run shows `SortMergeJoin`, but the old plan showed `BroadcastHashJoin`. The dimension grew. Either re-broadcast with a hint and a guardrail, or accept the new strategy and tune accordingly.

What to look for in the plan:

- `BroadcastExchange` present? If not, the small side did not get broadcast.
- `AdaptiveSparkPlan` `isFinalPlan=true` with a different operator in the final plan? AQE may have rewritten it; trust the final plan.

What to look for in the Spark UI:

- Compare shuffle bytes between runs for the same stage ID. Even if the stage IDs differ, the SQL operator ID is stable.
- Compare task counts. A surprise jump from 200 tasks to 2000 in the join's reduce side usually means AQE coalescing was disabled or shuffle partitions were overridden.

Better version: put a soft contract in the code that fails fast if the dimension exceeds the broadcast budget. Even a single `count()` and an assertion before the join is cheaper than a 90-minute incident.

## Production Smells

Treat any of these as a red flag during a code or design review:

- **Broadcasting because "it's a dimension."** Dimension is a logical role, not a size guarantee. Always validate bytes.
- **Joining on `cast(x as string)` or similar.** Casts on join keys can disable hash partitioning and force a worse plan. Fix the schema, not the query.
- **A dimension that is "small enough" with no monitoring.** Set a row-count or byte alert on every broadcastable dimension.
- **No filter on the partitioned column before the join.** Partition pruning is the largest possible optimization. If it is missing, fix that first.
- **`SELECT *` with a join.** You are paying shuffle and sort for columns nobody reads.
- **No statistics on a heavily-joined table.** Run `ANALYZE TABLE ... COMPUTE STATISTICS` for size; `... FOR COLUMNS` for selectivity. Stale stats are the #1 silent regression cause.
- **`MERGE INTO` against the entire history table without a partition predicate.** This is a join in disguise, and it is the most common "shuffle ate the cluster" pattern in lakehouse workloads.
- **A join hint without a comment explaining why.** Hints are platform contracts. Without a comment, they will outlive the reason they were added.

## Bad vs Better Join Patterns

| Problem | Bad Fix | Better Fix |
| --- | --- | --- |
| Sort-merge chosen for what should be broadcast | Increase executor memory | Add `/*+ BROADCAST(t) */` and a size guardrail |
| Broadcast OOM on driver/executor | Lower memory limits and retry | Switch to sort-merge with `/*+ MERGE(t) */`; add monitoring on the build side bytes |
| Slow large-large join | More executors | Pre-filter both sides; add semi-join to narrow the larger side |
| Skew on join key | Increase shuffle partitions | AQE skew join; if not enough, salt the hot key (see Chapter 5) |
| Output 10x expected rows | Check input row counts | Audit duplicate keys on build side: `GROUP BY join_key HAVING COUNT(*) > 1` |
| Cast in join condition | Wrap in `try_cast` | Fix the schema; cast at ingest, not at every join |
| Plan shows `BroadcastNestedLoopJoin` | Increase broadcast threshold | The condition is non-equi; rethink the query, or restrict one side aggressively |
| Stale stats causing wrong strategy | Hint everywhere | Schedule `ANALYZE TABLE` on the small/medium tables; remove hints once stats are reliable |
| Many-to-many join needed | Live with it | Pre-aggregate one side first; join 1:N with N pre-aggregated |

## Worked Example — Null Join Keys And Type Mismatch

Two specific bugs are worth showing because they are silent.

```sql
-- Bug 1: null join keys
SELECT count(*)
FROM orders o
JOIN customers c
  ON o.customer_id = c.customer_id;

-- If 5% of orders have null customer_id, those rows are silently dropped.
-- The join "works" but the result is wrong.
```

Diagnosis: count nulls on the join key on both sides before tuning.

```sql
SELECT
    sum(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_keys,
    count(*) AS total
FROM orders;
```

Fix: decide the contract. Either filter explicitly (`WHERE customer_id IS NOT NULL`) or use a sentinel and outer join (`LEFT JOIN` with `coalesce(c.country_name, 'unknown')`).

```sql
-- Bug 2: type mismatch
-- orders.customer_id BIGINT
-- customers.customer_id STRING

SELECT *
FROM orders o
JOIN customers c
  ON o.customer_id = c.customer_id;
```

The plan will show `cast(customer_id#10 as string)` (or similar). The cast can disable hash-based shuffle co-partitioning, slow the join, and reduce the chance AQE makes it broadcast. Fix the schema at ingestion, not in every query that touches it.

## Staff-Level Review Checklist

Before approving a Spark SQL change that includes a join, confirm each of these. If you cannot confirm one from the plan or the code, that is the next question to ask.

- [ ] Join condition is equi on a single, well-typed key.
- [ ] Join key types match exactly on both sides — no implicit cast.
- [ ] Null behavior on the join key is intentional and documented.
- [ ] Duplicate keys on the build side are accounted for, or proven absent.
- [ ] Filters on partitioned source columns are present and pushed down (`PartitionFilters` in the plan).
- [ ] Only the columns needed downstream are projected before the join.
- [ ] Broadcast hints are paired with a size guardrail (row count or byte budget).
- [ ] Strategy in the final plan matches the design intent (broadcast vs sort-merge).
- [ ] AQE is enabled, and skew join handling is enabled if the workload has a known hot-key risk.
- [ ] Statistics are available for any table participating in non-broadcast joins.
- [ ] Output row count is bounded by an explicit assertion or a downstream quality check.
- [ ] The job persists Spark event logs so the plan can be inspected after the cluster terminates.

## Anti-Patterns

- Treating a join hint as a permanent fix. Hints work around missing stats. Fix the stats; remove the hint.
- Bumping `spark.sql.autoBroadcastJoinThreshold` cluster-wide to "encourage" broadcast. This is how you OOM other people's jobs.
- Writing nested SQL with three derived joins where one wide CTE would have been clearer and let Catalyst optimize globally.
- Caching one side of a join "to make the join faster." Caching is a memory cost, not a free speedup. Validate it actually helps with the SQL tab.
- Letting `BroadcastNestedLoopJoin` ship to production because "it works." It works until the small side grows.

## Real Use Case

A customer 360 pipeline joins three things daily: 3 TB of transactions, a 20 MB country dimension, and a 600 GB customer profile table. The job had been stable for nine months at 25 minutes.

A new engineer added a `customer_address` table to enrich the profile. The address table was 70 MB. Default broadcast threshold is 10 MB, so it would not be broadcast automatically. The engineer added `spark.sql.autoBroadcastJoinThreshold=536870912` (512 MB) to the job, and everything was fast.

Three weeks later, an unrelated change caused the customer profile table to land in the optimizer's broadcast-eligible bucket, because column statistics suggested it might be small after some predicate pushdown. The optimizer happily broadcast a 280 MB hash relation. Driver heap pressure killed the job.

The lesson: cluster-level threshold changes are a contract with every job in the cluster. Either set the threshold per job, or add hints with explicit budget guards. The team rolled the threshold back, added `/*+ BROADCAST(country) */` and `/*+ BROADCAST(address) */`, and added pre-job assertions on the dimension byte size.

The fix took twenty minutes. The lesson — that threshold knobs are platform contracts — survived the team rotation.
