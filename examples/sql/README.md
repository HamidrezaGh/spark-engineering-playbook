# Spark SQL Examples

These examples support the chapters on execution model, shuffle, joins, skew, and adaptive query execution. Each file is annotated with what it demonstrates, what to look for in the Spark UI, what physical plan operators matter, and which production issue it helps diagnose.

The examples are written to be readable on their own — open one file and you should understand the full intent without external context.

## Files

| File | Demonstrates | Pairs With |
| --- | --- | --- |
| [`01-explain-shuffle.sql`](01-explain-shuffle.sql) | How to read a physical plan and find shuffle boundaries, partition pruning, and column pruning. | Chapter 1 (Execution Model), Chapter 2 (Shuffle And Performance). |
| [`02-broadcast-vs-sort-merge-join.sql`](02-broadcast-vs-sort-merge-join.sql) | How Spark picks a join strategy, how to confirm it from `EXPLAIN`, and how to force or disable broadcast. | Chapter 4 (Joins), Chapter 19 (Statistics And CBO). |
| [`03-skew-detection.sql`](03-skew-detection.sql) | How to measure key skew with simple SQL before it becomes a long-tail stage. | Chapter 5 (Data Skew), Chapter 6 (AQE). |
| [`04-window-vs-groupby.sql`](04-window-vs-groupby.sql) | When to use a window function vs `GROUP BY` + `JOIN`, and how the physical plan differs. | Chapter 4 (Joins), Chapter 9 (Spark SQL And Catalyst). |

## How To Use These

These are reference examples, not runnable benchmarks. Adapt the table names to your environment and run `EXPLAIN FORMATTED` first; only then run the actual query. The point is to build the habit of reading plans before tuning.
