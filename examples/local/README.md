# Local Examples

A small, runnable harness for the SQL and PySpark examples in this repo. The goal is to give you something you can launch on a laptop in five minutes, look at the Spark UI, and see plans, stages, and partitions on real (tiny) data.

This is **not** a benchmark. The sample data is intentionally small. The point is to read `EXPLAIN FORMATTED` output, watch tasks in the Spark UI, and build the muscle memory the rest of the handbook expects.

## What's Here

| File / Directory | Purpose |
| --- | --- |
| `data/events_sample.csv` | ~50 fact-style event rows with deliberate skew on `cust_flagship`. |
| `data/customers_sample.csv` | ~22 dimension-style customer rows with a couple of nulls and edge cases. |
| `run_examples.sh` | A driver script that registers the CSVs as temp views and runs the SQL examples (and optionally the PySpark scripts). |

The CSVs are tiny on purpose. They show the *shape* of the production patterns the chapters discuss — a hot key, null join keys, a small dimension that may or may not be broadcast — without needing real data.

## What You Need

- Apache Spark 3.4 or newer installed locally, with `spark-sql` and `spark-submit` on your `PATH`.
- Python 3.9+ for the PySpark scripts.
- PySpark installed in the active Python environment if you want to run the PySpark examples (`pip install pyspark`).

If you do not have Spark installed, the fastest path is:

```bash
# Install via pip; this gives you spark-sql, spark-submit, and pyspark.
pip install pyspark
```

That ships a usable local Spark for these examples. You will not get the standalone CLI scripts on PATH unless you also install the binary distribution; for the SQL examples the binary distribution is preferred.

## Running The Examples

From this directory:

```bash
# Run all SQL and PySpark examples in sequence:
./run_examples.sh

# Run only the SQL examples (uses spark-sql):
./run_examples.sh sql

# Run only the PySpark inspection scripts:
./run_examples.sh pyspark
```

While Spark is running, open `http://localhost:4040` in your browser to inspect the Spark UI. The UI is only alive while a Spark driver is running; for SQL examples each `spark-sql` invocation is a separate driver and a separate UI session.

If you want the UI to survive after the job finishes, set `spark.eventLog.enabled=true` and a writable `spark.eventLog.dir`, then start the History Server separately. That is overkill for these examples.

## What Each Example Demonstrates

| Example | Pairs With | What To Look For |
| --- | --- | --- |
| `examples/sql/01-explain-shuffle.sql` | Chapter 1 (Execution Model), Chapter 2 (Shuffle And Performance) | `Exchange hashpartitioning` in the plan — that is the stage boundary. Confirm `PartitionFilters` and `ReadSchema` show pruning on the source. |
| `examples/sql/02-broadcast-vs-sort-merge-join.sql` | Chapter 4 (Joins), Chapter 19 (Statistics And CBO) | `BroadcastHashJoin` vs `SortMergeJoin` in the plan. With small CSVs the dimension auto-broadcasts; experiment with `spark.sql.autoBroadcastJoinThreshold=-1` to force sort-merge and see the difference. |
| `examples/sql/03-skew-detection.sql` | Chapter 5 (Data Skew), Chapter 6 (AQE) | The top-key concentration query. `cust_flagship` should dominate. Read the row counts and compute the max-to-median ratio. |
| `examples/sql/04-window-vs-groupby.sql` | Chapter 4 (Joins), Chapter 9 (Spark SQL And Catalyst) | Compare the physical plans of a window function vs a `GROUP BY` + `JOIN`. Note where the shuffles are. |
| `examples/sql/05-partition-pruning.sql` | Chapter 3 (Partitioning), Chapter 8 (File Formats), Chapter 9 (Spark SQL And Catalyst) | `PartitionFilters` and `PushedFilters` in the plan. With the local CSVs there are no real partitions, but the example shows what to expect on a partitioned source. |

The PySpark scripts (`examples/pyspark/inspect_partitions.py`, `examples/pyspark/skew_detector.py`) accept either a registered Spark table (`--table`) or a local file (`--input` plus `--format`). The `run_examples.sh` script invokes them in file-input mode against the sample CSVs.

## What To Look For In Spark UI

Once you have a driver running:

1. Open `http://localhost:4040`.
2. Click the **SQL / DataFrame** tab. Find the query you just ran. Click into the operator graph.
3. Identify the `Exchange` nodes — those are stage boundaries.
4. Identify the join operator (`BroadcastHashJoin` or `SortMergeJoin`) and read its annotations.
5. Click the **Stages** tab. For each stage, look at:
   - Task count.
   - Per-task duration distribution (Summary Metrics).
   - Shuffle read / write bytes.
6. Click the **Executors** tab. Confirm there is one local executor and the driver is healthy.

For tiny CSV examples most of these metrics will be small. The point is the *shape* of the plan and the *names* of the operators, not the numeric values. Reading the same shape on a 10× or 1,000× larger dataset is the same skill.

## Optional: docker-compose

For users who do not want to install Spark locally, `docker-compose.yml` (if present) can run a single-node Spark in a container. The CSVs are mounted into the container, and you can `docker exec` into it to run `spark-sql` against the same temp views.

This is intentionally optional. A local `pip install pyspark` is simpler for most readers.

## Caveats

- These examples will not show interesting AQE behavior on this little data. AQE thresholds default to MB-scale; CSVs in the 1 KB range will not trigger AQE rewrites. To exercise AQE behavior, point the scripts at a real partitioned dataset using the `--input` or `--table` flags.
- Sort-merge vs broadcast decisions on these CSVs will almost always pick broadcast because everything fits well under the threshold. To force sort-merge, set `spark.sql.autoBroadcastJoinThreshold=-1` in the SQL session and re-run.
- The `events_sample.csv` is not partitioned by `event_date` on disk; the SQL examples that reference partition pruning describe what would happen on a partitioned source, not what the local CSV actually does.

The chapters they pair with explain what production-scale behavior looks like; the local examples give you a reproducible plan to read while you learn.
