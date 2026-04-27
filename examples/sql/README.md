# Spark SQL Examples

These examples support the chapters on execution model, shuffle, joins, skew, AQE,
partitioning/pruning, and Iceberg. Each file is annotated with what it demonstrates, what to look
for in the Spark UI, what physical plan operators matter, and which production issue it helps
diagnose.

The examples are written to be readable on their own — open one file and you should understand the
full intent without external context.

## Files

### [`01-explain-shuffle.sql`](01-explain-shuffle.sql)

- **Demonstrates:** How to read a physical plan and find shuffle boundaries, partition pruning,
  and column pruning.
- **Pairs with:** Chapter 1 (Execution Model), Chapter 2 (Shuffle And Performance).

### [`02-broadcast-vs-sort-merge-join.sql`](02-broadcast-vs-sort-merge-join.sql)

- **Demonstrates:** How Spark picks a join strategy, how to confirm it from `EXPLAIN`, and how to
  force or disable broadcast.
- **Pairs with:** Chapter 4 (Joins), Chapter 19 (Statistics And CBO).

### [`03-skew-detection.sql`](03-skew-detection.sql)

- **Demonstrates:** How to measure key skew with simple SQL before it becomes a long-tail stage.
- **Pairs with:** Chapter 5 (Data Skew), Chapter 6 (AQE).

### [`04-window-vs-groupby.sql`](04-window-vs-groupby.sql)

- **Demonstrates:** When to use a window function vs `GROUP BY` + `JOIN`, and how the physical plan
  differs.
- **Pairs with:** Chapter 4 (Joins), Chapter 9 (Spark SQL And Catalyst).

### [`05-partition-pruning.sql`](05-partition-pruning.sql)

- **Demonstrates:** Verifying partition pruning, column pruning, and predicate pushdown from
  `EXPLAIN`, with the common ways each silently breaks.
- **Pairs with:** Chapter 3 (Partitioning), Chapter 8 (File Formats), Chapter 9 (Spark SQL And
  Catalyst).

### [`06-aqe-in-action.sql`](06-aqe-in-action.sql)

- **Demonstrates:** Observing what AQE does at runtime: coalesce, dynamic switch to broadcast, skew
  join handling.
- **Pairs with:** Chapter 6 (AQE), Chapter 4 (Joins), Chapter 5 (Data Skew).

### [`07-iceberg-merge.sql`](07-iceberg-merge.sql)

- **Demonstrates:** A scoped Iceberg `MERGE` with bounded predicates, snapshot inspection,
  validation gate, and rollback.
- **Pairs with:** Chapter 13 (Iceberg And Spark), Chapter 24 (Incremental Processing And
  Backfills), [`docs/case-studies/emr-merge-memory-spill.md`](../../docs/case-studies/emr-merge-memory-spill.md).

### [`join-strategies/`](join-strategies/README.md)

- **Demonstrates:** Short `EXPLAIN` pair: **broadcast** vs **sort-merge** on the same join.
- **Pairs with:** Chapter 4 (Joins), [`docs/observability/physical-plans.md`](../../docs/observability/physical-plans.md).

### [`iceberg-write-path/`](iceberg-write-path/README.md)

- **Demonstrates:** MERGE / **rewrite** / write-distribution **templates** (requires Iceberg).
- **Pairs with:** Chapters 13, 17, [`docs/troubleshooting/iceberg-merge-issues.md`](../../docs/troubleshooting/iceberg-merge-issues.md).

## How To Use These

These are reference examples, not runnable benchmarks. Adapt the table names to your environment and
run `EXPLAIN FORMATTED` first; only then run the actual query. The point is to build the habit of
reading plans before tuning.

A reasonable order if you are working through the book:

1. `01` and `05` after Chapter 1 / Chapter 3 — the basic plan-reading and pruning vocabulary.
2. `02` and `04` after Chapters 4 and 9 — join strategy and query shape.
3. `03` and `06` after Chapters 5 and 6 — skew detection paired with AQE behavior.
4. `07` after Chapter 13 / Chapter 24 — Iceberg `MERGE` with the operational guardrails the case
   study describes.

## Screenshot placeholders (docs)

The handbook’s Markdown pages use **PNG placeholders** for Spark UI and `EXPLAIN` samples; see
[`../../docs/assets/screenshots/README.md`](../../docs/assets/screenshots/README.md). When you run
these SQL files locally, capture your own (non-confidential) UI and plan images to compare against
the teaching captions in
[`../../docs/observability/physical-plans.md`](../../docs/observability/physical-plans.md) and
[`../../docs/observability/spark-ui-guide.md`](../../docs/observability/spark-ui-guide.md).
