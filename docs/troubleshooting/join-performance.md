# Troubleshooting: join performance

**Problem:** Joins are slow, spill during join, or unexpected `BroadcastHashJoin` / `SortMergeJoin` choice.

## Symptoms

- Join stage is largest share of job time.
- **EXPLAIN** shows `SortMergeJoin` when you expected broadcast (or the opposite).
- OOM on broadcast or on sort-merge with huge cardinality.
- Skew only on the join output (max task time ≫ median) — see [skew-and-stragglers](skew-and-stragglers.md).

## What to check first

1. **EXPLAIN FORMATTED** — `BroadcastHashJoin`, `SortMergeJoin`, or `ShuffledHashJoin`? Any `Exchange` on both sides?
2. **Table sizes and stats** — missing stats often cause default sort-merge.
3. **Key types and nulls** — null handling changes semantics and can explode cardinality.
4. **Buckling / pre-shuffle** — are both sides already distributed on the same key?

## Spark UI and SQL

- **SQL** tab: join node details; AQE `AdaptiveSparkPlan` may rewrite strategy at runtime.
- **Stages:** shuffle read around the join; spill on build or probe side.
- **Broadcast:** watch **driver** memory and **BroadcastExchange** row counts.

| Pattern in plan | Typical meaning |
| --- | --- |
| `BroadcastHashJoin` with large `BroadcastExchange` | Risk: broadcast too big or stats wrong |
| `SortMergeJoin` + two large `Exchange` | Two big shuffles; ensure filters applied before |
| `ShuffledHashJoin` | Less common; memory-sensitive — watch spill |

## Logs and metrics

- Stats collection: are `ANALYZE TABLE` or Iceberg/Delta stats current?
- Warnings about **broadcast** timeout or OOM in driver/executor logs.

## Likely causes

- **Missing or stale statistics** — broadcast threshold never triggers correctly.
- **Skewed join key** — one key dominates after shuffle.
- **Join order** — large table joined first, carrying too many columns.
- **Undef pre-filter** — both sides are “full table” when one could be pruned.
- **Type coercion** on keys preventing pushdown or bucket alignment.

## Fix options

- **Filter and project** before the join; smallest row/column set possible.
- **Refresh statistics**; fix **null-safe join** semantics if needed.
- **Broadcast** hints only when size is proven; otherwise prefer AQE and stats.
- **Skew** mitigations: AQE, salting, or key isolation.
- **Bucket join** (Hive/Iceberg) for recurring joins on shared keys.
- For **equi-join only** on large sides: `SortMergeJoin` is often correct — focus on **bytes moved**, not “avoid merge.”

## Tradeoffs

- Broadcast: zero shuffle on the small side, but driver + all executors copy data — caps apply.
- Sort-merge: stable for big data, but two shuffles and sort cost; sensitive to **skew** and **row width**.
- **Bucket table maintenance** complicates ETL but pays off on daily joins.

## Example final diagnosis

*Symptoms:* `SortMergeJoin` on `id` for “small” dimension. **Stats:** table metadata **size=unknown**. **ANALYZE** run — next run picks **broadcast**; stage time drops 6×. **Prevention:** stats refresh in the ingestion pipeline for dimensions.

## Prevention checklist

- [ ] Join keys reviewed in code review: types, nulls, and filters.
- [ ] Stats job or auto-stats for tables used in default paths.
- [ ] `EXPLAIN` captured in PR for new joins on large fact tables.
- [ ] Document when broadcast is forbidden (e.g. slowly growing “small” tables).

**See also:** [`../book/04-joins.md`](../book/04-joins.md), [`../configs/joins-and-broadcast.md`](../configs/joins-and-broadcast.md), [`../../examples/sql/join-strategies/README.md`](../../examples/sql/join-strategies/README.md).
