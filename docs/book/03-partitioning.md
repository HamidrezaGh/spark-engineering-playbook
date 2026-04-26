# Partitioning


## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- What “partition” means in Spark execution vs table storage layout.
- When to use `repartition()` vs `coalesce()` and what each costs.
- How partition count impacts task sizing, shuffle pressure, and output file count.
- How to spot partitioning issues in the Spark UI (tiny tasks, huge tasks, small files).
- How to avoid common table-partitioning mistakes (high cardinality, missing pruning).

## Core Idea

Partitioning controls how data is split for parallel work. Spark partitioning is about runtime task parallelism. Table partitioning in Hive, Glue, Iceberg, Delta, or Hudi is about how data is physically or logically organized for pruning, writes, and maintenance.

Confusing these two meanings is a common source of bad tuning.

## Key Takeaways

- **Spark partitions control task parallelism**.
- **Table partitions control storage layout and pruning**.
- **Too many partitions create overhead and small files**.
- **Too few partitions create large tasks, spill, and poor cluster utilization**.

## Mental Model

A Spark partition is a slice of a DataFrame or RDD processed by one task. More partitions usually means more tasks and more parallelism, but also more scheduling overhead and potentially more output files.

A table partition is a table layout concept, often based on columns or transforms such as day, month, bucket, or truncate. It helps readers skip irrelevant data and helps writers organize output.

| Type | Lives In | Controls | Common Mistake |
| --- | --- | --- | --- |
| Spark partition | Runtime execution plan | Task count and parallelism | Assuming it is the same as table partitioning |
| Shuffle partition | Runtime after wide transformations | Reduce-side task count | Leaving default `200` for every workload |
| Table partition | Storage/table metadata | Pruning and file organization | Partitioning by high-cardinality keys |

```text
Input files
  -> input partitions
  -> scan tasks
  -> exchange / shuffle
  -> shuffle partitions
  -> write tasks
  -> table partitions / output files
```

## What Spark Does Internally

Input partitions are created from file splits, file sizes, source partitions, and scan planning settings. Shuffle partitions are created after wide transformations such as joins and aggregations.

`repartition(n)` creates a shuffle and returns exactly or approximately `n` partitions depending on API and execution details. `repartition(col)` shuffles by column values so rows with the same partitioning expression land together. `coalesce(n)` usually reduces partitions without a full shuffle, which is cheaper but can create uneven partitions.

## Why It Matters In Production

Partition count drives:

- Task parallelism.
- Per-task memory pressure.
- Shuffle block count.
- Output file count.
- Scheduler overhead.
- Ability to use the cluster efficiently.

Too many partitions create tiny tasks, scheduler overhead, small files, and unnecessary metadata pressure. Too few partitions create large tasks, poor parallelism, memory pressure, spill, and long job runtimes.

## Production Smells

- Too many tiny tasks.
- A few huge tasks.
- Thousands of small output files.
- Partition pruning is expected but does not happen.
- Cluster cores are idle while a few large tasks run.
- A job spends more time scheduling tasks than processing data.

## Common Failure Modes

- Small-file explosion after writing with too many final partitions.
- OOM or spill from too few partitions before a join or aggregation.
- Skewed partitions caused by uneven key distribution.
- Slow planning from listing too many files or partitions.
- Ineffective table partitioning because filters do not match partition columns or transforms.

## Tuning And Configuration

Use `repartition(n)` when you need more or differently distributed parallelism and can justify the shuffle. Use `repartition(col)` when downstream work benefits from colocating rows by key, such as joins, aggregations, or partitioned writes. Use `coalesce()` when reducing partitions after a filter or before a small write, and uneven partition sizes are acceptable.

Practical checks:

- Compare task count to available executor cores.
- Check task input sizes and duration spread.
- Check output file sizes after writes.
- Use AQE to coalesce small shuffle partitions where appropriate.
- Tune table partitioning for query filters, not for every high-cardinality column.

## Spark UI Signals

Look at:

- Number of tasks per stage.
- Input size per task.
- Task duration percentiles.
- Shuffle partition sizes.
- Output file count after write jobs.
- SQL plan `Exchange` nodes caused by `repartition`.

## Best Practices

- Separate runtime partitioning decisions from table layout decisions.
- Repartition intentionally before expensive wide operations or writes.
- Avoid table partitioning on very high-cardinality columns unless using a table format transform designed for it.
- Target output file sizes that balance read efficiency and write parallelism.
- Profile key distributions before repartitioning by column.

## Anti-Patterns

- Calling `repartition(1)` to create a single file in production.
- Partitioning a table by user ID, request ID, or another high-cardinality key without a strong reason.
- Using `coalesce()` to fix skew.
- Setting partition counts without checking task sizes or output file sizes.

## Example

```python
events = spark.read.parquet("s3://lake/events/")

daily = events.filter("event_date = '2026-04-25'")

(
    daily.repartition("event_date")
         .write
         .mode("overwrite")
         .partitionBy("event_date")
         .parquet("s3://lake/events_by_day/")
)
```

The `repartition("event_date")` affects Spark runtime distribution. The `partitionBy("event_date")` affects the table/file layout. They are related in this write path, but they are not the same concept.

## Interview-Style Questions Covered

- What is the difference between Spark partitioning and table partitioning?
- What is the difference between `repartition()` and `coalesce()`?
- When would you use `repartition(n)`?
- When would you use `repartition(col)`?
- When would you use `coalesce()`?
- Can too many partitions hurt performance?
- Can too few partitions hurt performance?
- How do you detect partition skew?
- What is the difference between input partitions and shuffle partitions?
- How does Spark read many small files?

## Real Use Case

A clickstream pipeline writes 40,000 Parquet files per day because the final DataFrame has too many partitions and writes into many table partitions. Downstream queries slow down because scan planning and object-store listing become expensive. The fix is to align final Spark partitions with target file sizes, compact old small files, and choose table partition transforms based on common query filters such as event date and region.
