# Troubleshooting: shuffle-heavy job

**Problem:** Shuffle read/write bytes are huge, shuffle stages dominate runtime, or the cluster is “network bound.”

## Symptoms

- Stages with very large **Shuffle Read** and **Shuffle Write** in the summary.
- **Fetch wait** or low CPU with high shuffle time.
- `FetchFailedException` in logs (sometimes coupled with [emr-yarn-failures](emr-yarn-failures.md)).
- `spark.sql.shuffle.partitions=200` (or default) on multi-TB shuffles.

## What to check first

1. **EXPLAIN (formatted)** — count `Exchange` nodes; each is a shuffle boundary and cost.
2. **What operators?** `join`, `groupBy`, `distinct`, `orderBy` / window all multiply bytes moved.
3. **Can you filter/project earlier?** The cheapest shuffle is the one you remove.
4. **Join strategy** — `BroadcastHashJoin` vs `SortMergeJoin` (see [join-performance](join-performance.md)).

## Spark UI signals

- Shuffle **read** and **write** totals on the hot stage.
- **SQL** tab: `Exchange` → links to the expensive stage; note partitioning columns.
- **Input** to the first shuffle stage: unnecessary scan volume upstream.

## Logs and metrics

- Spark metrics: `shuffleBytesRead`, `shuffleBytesWritten` (if exported).
- For EMR: network metrics on instances during shuffle-heavy windows.

## Likely causes

- **Cartesian** or many-to-many join explosion (missing join condition, wrong key).
- **Join** of two large tables without a selective predicate.
- **Repartition** / **join** in the wrong order — wide data carried through multiple shuffles.
- **Too many shuffle partitions** on small data — lots of small tasks (opposite problem but still “shuffle heavy” in *count*).
- **Unnecessary `distinct` or global sort** before a cheaper alternative exists.

## Fix options

- **Predicate pushdown** and early **filter** / **select** in SQL or the DataFrame API.
- **Broadcast** the genuinely small side (verify with size stats, not guess).
- **Pre-aggregate** on join keys to shrink row counts.
- **Bucket** or **partition** common join keys in the table design (longer-term).
- **Tune** `spark.sql.shuffle.partitions` and AQE post-shuffle coalescing; validate with a test run.
- **Split the pipeline** so intermediate results are materialized in a healthy layout (Iceberg) instead of re-shuffling the same data daily.

## Tradeoffs

- Lower shuffle partition count: larger tasks, spill risk, longer recovery from skew.
- Broadcast: fast until it isn’t — driver/executor OOM if threshold is wrong.
- **Caching** a large shuffle output can avoid recomputing *if* the reuse is real.

## Example final diagnosis

*Symptoms:* 1.2 TB shuffle read on a daily job. **Plan:** two `Exchange` nodes back-to-back after reading wide tables. **Root cause:** missing filter on `event_date` — scan sent full history into both shuffles. **Fix:** add partition filter; shuffle dropped 90%+. **Prevention:** SQL review checklist for partition columns on fact reads.

## Prevention checklist

- [ ] Every fact scan has **partition** or **prune** conditions where the table is partitioned.
- [ ] `EXPLAIN` checked when changing joins or `GROUP BY` keys.
- [ ] AQE on for all SQL workloads (unless a rare exception is documented).
- [ ] No duplicate shuffles on the same wide DataFrame without `cache` + measured reuse.

**See also:** [`../book/02-shuffle-and-performance.md`](../book/02-shuffle-and-performance.md), [`../tuning/shuffle-partitions.md`](../tuning/shuffle-partitions.md).
