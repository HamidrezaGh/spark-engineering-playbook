# Reading Spark physical plans

This page ties **Catalyst** plan shapes to what runs on the cluster. Pair it with
`EXPLAIN (cost)` or `EXPLAIN FORMATTED` and the **SQL** tab in the Spark UI.

- **Logical plan** — what the query *means* (unresolved → analyzed → optimized).
- **Physical plan** — what Spark *will run*: scans, shuffles, joins, and codegen boundaries.

`EXPLAIN FORMATTED` (Spark SQL) is usually easier to read than a flat tree. The **SQL** tab in the
Spark UI shows the same operator family after the job runs; align those nodes with the slow stage
you picked in **Stages**.

![Placeholder: EXPLAIN or SQL tab fragment showing Exchange, join strategy, and AdaptiveSparkPlan](../assets/screenshots/placeholder-explain-physical-plan.png)

Caption: **Physical plan** reading is easier when you scan for **scans** (filters/pushdown), each **Exchange** (shuffle / broadcast), and **join/aggregate** choice — then match that subtree to the expensive **stage** in the UI.

Example:

```sql
EXPLAIN FORMATTED
SELECT c.region, count(*)
FROM events e
JOIN customers c ON e.customer_id = c.id
GROUP BY c.region;
```

In PySpark, `df.explain("formatted")` is equivalent.

## Common physical operators (what to look for)

| Operator / phrase | What it does | Why it shows up in incidents |
| --- | --- | --- |
| **`FileScan` / `BatchScan` / `Iceberg scan`** | Read files / metadata | Missing `PushedFilters` or `PartitionFilters` ⇒ full scan cost |
| **`Filter` (after scan)** | Row-level predicate | You want *pushed* filters under scan when the format allows |
| **`Project`** | Column subset | **ReadSchema** too wide ⇒ I/O and memory waste |
| **`Exchange` / `ShuffleExchange`** | Shuffle; stage boundary | Each `Exchange` is usually a new **stage** |
| **`HashPartitioning` / `RangePartitioning`** | How shuffle routes rows | Affects **skew** and sort requirements |
| **`BroadcastExchange`** | Ship small table to all executors | OOM on driver/executor if the broadcast is “too big in practice” |
| **`BroadcastHashJoin` (BHJ)** | **Build** = broadcast, **stream** = scan shuffle once | **No shuffle of the build side**; fast for truly small build |
| **`SortMergeJoin` (SMJ)** | Sort both sides, merge | Common for large / unknown-size sides; shuffles on join keys if not pre-sorted |
| **`ShuffledHashJoin` (SHJ)** | Build hash on one shuffled side | Memory-sensitive; can be chosen under cost model when sort can be avoided |
| **`Sort`** | **Full** or prefix sort | Extra CPU + may require shuffle; watch *global* sort |
| **`HashAggregate` / `ObjectHashAggregate` / `SortAggregate`** | Partial / final aggregate | `Hash` usually preferred; `SortAggregate` on wide keys can spill |
| **`Window`** | Partition + frame | Forces shuffle on **partition by**; expensive if partition spec is high-card |
| **`AdaptiveSparkPlan` / `QueryStage`** | AQE re-plans with runtime sizes | Re-read **after** the query runs; initial vs final can differ |
| **`ReusedExchange` / `InMemoryTableScan`** | Reuse a shuffle or broadcast | **Good** (less work); be sure the reuse is intentional and not a hint mistake |
| **`WholeStageCodegen` (codegen: true / false)** | Fuse operators into generated loops | Fused `*` blocks are the **codegen** regions — faster when present |
| **`Filter` under scan `PushedFilters` / `PartitionFilters`** | Predicate pushdown | `PartitionFilters` for **table** partitions; `PushedFilters` for file/rowgrps |

> Spark version strings vary slightly. Treat names as *family* (merge vs hash vs broadcast), not
> byte-identical text across 3.4 vs 3.5.

## EXPLAIN: quick patterns

**Broadcast join** (small dimension):

```text
+- BroadcastHashJoin [customer_id#...], [id#...], ...
   :- BroadcastExchange ...
   :  +- Filter (...)
   :     +- FileScan ... customers ...
   +- Filter (...)
      +- FileScan ... events ... PushedFilters: [isnotnull(...)], ...
```

**Sort-merge join** (two big sides):

```text
+- SortMergeJoin [k#1], [k#2], ...
   :- Sort ...
   :  +- Exchange hashpartitioning(k#1, ...)   -- shuffle
   +- Sort ...
      +- Exchange hashpartitioning(k#2, ...)  -- second shuffle
```

**Shuffle and aggregate**:

```text
+- HashAggregate(keys=[region#...], ...)
   +- Exchange hashpartitioning(region#..., ...)
      +- HashAggregate(... functions=[partial_count(...)])   -- map-side combiners
         +- FileScan ... events ... PartitionFilters: [dt#... IN (...)]
```

## AQE in the plan

- **Initial** physical plan is printed at planning time.
- **Runtime** rewrites (join swap, coalesce shuffle partitions, skew, local shuffle) appear when
  `spark.sql.adaptive.enabled=true` (on by default in Spark 3+ for SQL).
- In the **SQL** UI, compare **“Logical Plan”** vs the **final** physical plan, or re-run
  `EXPLAIN` after execution if you capture the replay from history server.

**See:** [`../book/06-adaptive-query-execution.md`](../book/06-adaptive-query-execution.md),
[`../configs/aqe.md`](../configs/aqe.md).

## Reused exchange

When a subtree is used twice, Spark may run one **Exchange** and **reuse** the shuffle output.

**Check:** the plan shows `ReusedExchange` or shared exchange ids. This is good when it matches
the intent; it is bad if you *thought* the branch had different filters but it did not.

## WholeStageCodegen

- Nested operators inside a `*`-marked block run in one generated function — low virtual-call
  overhead.
- UDFs and some expressions **break the pipeline**; you may see **`ExternalRDD`**,
  `pythonUDF*`, or **no codegen** in that region.

**See:** [`../book/09-spark-sql-and-catalyst.md`](../book/09-spark-sql-and-catalyst.md).

## Pushed and partition filters

- **`PushedFilters`** — predicates pushed into Parquet/Orc/JSON readers (row groups, stats).
- **`PartitionFilters`** — partition pruning (catalog / directory layout / Iceberg partition spec).
- If your **runtime** is huge but the **scan** line lacks filters you expected, fix the *query* or
  **table layout**, not the cluster first.

**Runnable examples:** [`../../examples/sql/01-explain-shuffle.sql`](../../examples/sql/01-explain-shuffle.sql),
[`../../examples/sql/02-broadcast-vs-sort-merge-join.sql`](../../examples/sql/02-broadcast-vs-sort-merge-join.sql),
[`../../examples/sql/join-strategies/README.md`](../../examples/sql/join-strategies/README.md).

## How this connects to the Spark UI

| Plan spot | Stages / SQL tab |
| --- | --- |
| Count **Exchange** nodes | ≈ number of **shuffle** boundaries / stages to double-check |
| `BroadcastHashJoin` | `BroadcastExchange` time and size in **SQL**; driver memory in **Executors** |
| `SortMergeJoin` + two `Exchange` | **Shuffle read** bytes in the two incoming stages |
| `FileScan` with no `PartitionFilters` | **Input** bytes looks like “full table” on partitioned table |

**Related:** [`spark-ui-guide.md`](spark-ui-guide.md),
[`../book/01-execution-model.md`](../book/01-execution-model.md),
[`../book/04-joins.md`](../book/04-joins.md).
