# Chapter 12 — Production Debugging

Production debugging is evidence collection under time pressure. The on-call engineer is not paid to be smart; they are paid to be systematic. The Spark UI, the persisted event logs, and the runtime logs (YARN, EMR step, CloudWatch) hold the evidence. Tuning knobs do not.

This chapter is a runbook. It walks through a real-shaped incident — "the job was 20 minutes yesterday and 2 hours today" — step by step, and it lists the symptom-to-evidence-to-cause tables you would actually use during a 2 AM page.

## What You Should Be Able To Answer

After this chapter, you should be able to answer quickly, from memory:

- What evidence do you collect first when a job is slower than yesterday, before changing any config?
- How do you locate the slowest stage and the slowest task within it, without reading every log line?
- How do you tell "this is a data shape change" from "this is a runtime issue" from "this is a code issue"?
- Where do you read the physical plan after the cluster has terminated?
- What is the smallest safe fix for each common production symptom?
- What guardrail or metric do you add so this incident is faster to diagnose next time?

## Mental Model — Four Layers Of A Spark Incident

Every production Spark incident lives in one of four layers. Diagnose the layer first; the fix is layer-specific.

| Layer | What Lives Here | Evidence Sources |
| --- | --- | --- |
| Code | SQL, plan, joins, UDFs, writes | SQL tab, EXPLAIN output, application code |
| Data | Volume, distribution, schema, file count, late records | Profile queries, source table metadata, file counts |
| Runtime | Executors, memory, cores, spill, GC | Stages tab, Executors tab, container logs |
| Platform | S3, network, EMR capacity, YARN queues, IAM | YARN logs, EMR step logs, CloudWatch, S3 metrics |

A surprising number of "Spark" incidents are platform incidents — S3 throttling, YARN queue starvation, EMR Spot reclamation. The Spark UI hints at this when the symptom is "everything is slow" without an identifiable expensive operator.

## The Triage Loop

The loop to run, in order, every time:

1. **What changed?** Code, data, config, cluster, dependencies, schedule, upstream input.
2. **Find the slowest or failed stage.** Not the slowest job — the slowest stage. Tuning is per stage.
3. **Read the plan.** Confirm the operators are what you expected.
4. **Look at task distribution.** Even? Long-tail? Concentrated spill?
5. **Check input data shape.** Row count, file count, top keys, nulls.
6. **Check runtime logs.** Container kills, fetch failures, GC, executor losses.
7. **Apply the smallest safe fix.** Change one thing. Re-measure.
8. **Add a guardrail.** A metric, an assertion, or a runbook update.

Skip step 1 and you are guessing. Skip step 7 and you cannot defend the change in a post-mortem. Skip step 8 and the next on-call engineer pays for it.

## Incident: Job Was 20 Minutes Yesterday, 2 Hours Today

This is the most common production incident shape: a job has been stable for months, the code has not changed, and now it is suddenly 5–10x slower. Walk through it as a real triage.

### Step 1 — Compare run metadata

Before opening the Spark UI, gather the metadata that lets you compare today to yesterday.

- **Application id and start/end times** for both the slow run and a recent good run.
- **EMR cluster details**: instance fleet composition, EMR release, any recent changes.
- **Source table snapshots**: what partitions were read, what was the row count and byte size, how many files.
- **Output table snapshots**: same questions for the output side.
- **Configuration diff**: anything in `spark.conf` that changed between runs. Pay particular attention to `spark.sql.shuffle.partitions`, `spark.sql.adaptive.*`, `spark.executor.memory`, `spark.executor.memoryOverhead`, and `spark.sql.autoBroadcastJoinThreshold`.

If your platform persists Spark event logs to S3 (`spark.eventLog.dir=s3://.../spark-event-logs/`), the slow run's full Spark UI is reproducible after the cluster has terminated. If it does not, fix that before continuing — non-persisted event logs is the single biggest avoidable mistake in production EMR.

| Metadata | Yesterday | Today | Diff |
| --- | --- | --- | --- |
| Wall-clock runtime | 22 min | 132 min | 6× slower |
| Input bytes | 1.2 TB | 1.4 TB | +17% |
| Input row count | 4.1 B | 4.6 B | +12% |
| Input file count | 8,400 | 21,000 | 2.5× more files |
| Output bytes | 380 GB | 410 GB | flat |
| EMR release | emr-6.15.0 | emr-6.15.0 | same |
| Spark version | 3.4.1 | 3.4.1 | same |
| Cluster size | 64 m5.4xlarge | 64 m5.4xlarge | same |
| `spark.sql.shuffle.partitions` | 800 | 800 | same |

The 6× runtime regression is not explained by 17% more bytes. The file count tripled. That is the most suspicious metadata signal, and it points at file-skew or scan planning cost.

### Step 2 — Find the slowest stage

Open the Spark UI. The Jobs tab gives you the top-level breakdown; the Stages tab gives you per-stage runtime. The slow stage is rarely the one you expect.

- Open the Stages tab.
- Sort by Duration descending.
- The top one or two stages are usually 80–95% of total runtime.

For the example incident, the slowest stage is the second join's reduce side, taking 95 minutes (out of 132). Yesterday's same stage took 12 minutes. That is the focus.

### Step 3 — Inspect SQL / physical plan

Open the SQL tab and click the query backing the slow stage. The visualization shows the operator graph; click each node to see runtime metadata.

- **Identify the operator that feeds the slow stage.** Is it `SortMergeJoin`, `HashAggregate`, `Window`, `Sort`, `BroadcastHashJoin`, `Exchange`?
- **Compare the static plan to the AQE final plan.** If `AdaptiveSparkPlan isFinalPlan=true` shows a different operator than the static plan, AQE intervened. Trust the final plan.
- **Look for missing pruning.** Is `PartitionFilters` populated on the source scan? Is `PushedFilters` populated? If either is empty when you expected it, fix that first.
- **Look at `numOutputRows` per node.** A row count explosion between two adjacent operators is usually a duplicate-key join or a missing filter.

For the incident: the slow stage is fed by a `SortMergeJoin` on `customer_id`. The plan looks identical to yesterday's plan. The static plan and the AQE final plan agree. Pruning fired on both sources. The plan is not the problem.

### Step 4 — Inspect task distribution

In the Stages tab for the slow stage, scroll to Summary Metrics and Tasks.

- **Summary Metrics → Duration → Max vs Median.** A 5–10× ratio is mild; 50× is a hot-key long tail.
- **Summary Metrics → Shuffle Read Size.** Concentrated read size confirms key-based skew.
- **Summary Metrics → Spill (Memory) and Spill (Disk).** Concentrated spill on a few tasks is a working-set problem.
- **Summary Metrics → Input Size and Records.** Concentrated input size is file-skew or split-skew.

For the incident:

- Median task duration: 18 seconds.
- Max task duration: 38 minutes.
- Max-to-median ratio: 127×. That is severe.
- Concentrated shuffle read on the slow tasks.

This is hot-key skew. The plan did not change; the data shape did.

### Step 5 — Check input data shape

The Spark UI told us where the time went. Now confirm the data shape changed in a way consistent with the symptom.

```sql
-- Top keys in the source table for today.
SELECT customer_id, COUNT(*) AS rows
FROM events
WHERE event_date = DATE '2026-04-25'
GROUP BY customer_id
ORDER BY rows DESC
LIMIT 10;

-- Same query for yesterday's data.
SELECT customer_id, COUNT(*) AS rows
FROM events
WHERE event_date = DATE '2026-04-24'
GROUP BY customer_id
ORDER BY rows DESC
LIMIT 10;
```

For the incident, the top key today (`acct_42`) has 480 million rows. Yesterday it had 18 million. A new use case launched on that account this morning. The total input grew 12% but the top-key share went from 0.4% to 10.4%. The skew is the cause.

The file count tripled because the upstream ingest emitted many small files for the burst on `acct_42`. That is a secondary symptom, not the cause of the slow stage — but it is its own incident waiting to happen.

### Step 6 — Check runtime logs

Look at logs only after the Spark UI has narrowed the question. Pulling 100 GB of executor logs into your terminal at 2 AM is not productive.

- **Driver logs**: `OutOfMemoryError`, plan compilation errors, `IllegalArgumentException` from configs, S3 access denied.
- **Executor logs**: `ExecutorLostFailure`, `FetchFailedException`, `OutOfMemoryError`, `Container killed by YARN for exceeding memory limits`.
- **YARN logs**: `Container killed on request`, `Killed external launch process`, queue wait events.
- **EMR step logs**: cluster events, instance fleet allocation, Spot reclamation events.
- **CloudWatch / S3 metrics**: 503 SlowDown counts, 5xx rates, request rate per prefix.

For the incident, executor logs show a few `Container killed by YARN for exceeding memory limits` on the slowest tasks. That is consistent with the long tail spilling and then exceeding the per-task working-set budget.

### Step 7 — Choose the smallest safe fix

The diagnosis is hot-key skew on a `SortMergeJoin`. Smallest safe fixes, ranked:

1. **Confirm AQE skew join is enabled.** It is. AQE may already be doing some splitting, just not enough.
2. **Lower `spark.sql.adaptive.skewJoin.skewedPartitionFactor`** and possibly `skewedPartitionThresholdInBytes`. This makes AQE more aggressive about splitting. This is the smallest possible change.
3. **Add a manual broadcast hint** if the build side is small enough. (For this example, it is not.)
4. **Apply hot-key isolation** for `acct_42` only. Process this account separately, reunion at the end.
5. **Apply two-phase aggregation with salting** if the operation is associative.

For this incident, option 2 reduced the long tail by 40% but did not eliminate it. Option 4 (isolating `acct_42`) brought the runtime back to ~28 minutes. The team chose option 4 with a comment in the SQL pointing to the underlying ticket.

### Step 8 — Add a guardrail

The fix does not finish at "it works again." It finishes at "the next time it happens, the on-call engineer is told before the SLA is at risk."

Guardrails added in this incident:

- **Top-key concentration metric.** A daily query on the source table emits the top-1 key share. An alert fires when it exceeds 5%.
- **Max-to-median task duration metric.** Emitted from the job after every run, written to a central metrics table. An alert fires when the heaviest stage's ratio exceeds 20×.
- **File count metric.** Emitted by the ingest job. An alert fires when daily file count exceeds the trailing 14-day median by 2x.
- **Runbook update.** This chapter, plus the diagnostic SQL queries, linked from the on-call rotation page.

The fix was 20 minutes of code. The guardrails were 90 minutes of code. The guardrails are why the team has not been paged for the same incident shape since.

## Symptom → Evidence → Cause Tables

These are the tables to print and keep nearby. They are organized by the first signal you see, because that is what the on-call engineer has at the start.

### Symptom: One Task Much Slower Than Others

| Evidence | Likely Cause | What To Check Next | Smallest Safe Fix |
| --- | --- | --- | --- |
| Max task duration ≫ median in slow stage | Hot-key skew | Profile top keys on the partitioning column | AQE skew join (lower thresholds) → salting → isolation |
| One task with much larger input size | File skew or split skew | Source file sizes; max input file size | Repartition before processing; fix upstream sizing |
| One task with much larger shuffle read | Hot-key skew on shuffle stage | Profile top keys on the shuffle key | Same as hot-key skew |
| One task spilling, others not | Per-task working set too large for that key's data | Same as above; verify per-task memory budget | Salt or split the hot key; consider more shuffle partitions |

### Symptom: Job OOMs

| Evidence | Likely Cause | What To Check Next | Smallest Safe Fix |
| --- | --- | --- | --- |
| Driver heap OOM | `collect()`/`toPandas()`; huge plans; huge file listing | Driver memory, plan size, listing scope | Replace `collect` with `write`; narrow listing; bigger driver heap as last resort |
| Executor OOM during shuffle | Per-task working set too large; bad broadcast | Slow task's shuffle read size; check `BroadcastExchange` size in plan | Increase shuffle partitions; remove or fix broadcast; check skew |
| Executor `Container killed by YARN` | Memory overhead exceeded | YARN log container kill reason; PySpark vs JVM memory split | Increase `spark.executor.memoryOverhead`; reduce per-task working set |
| OOM only on some tasks | Skew | Same as one-task-slow | Same as hot-key skew |
| OOM only in production | Driver memory differs; configs differ; data volume differs | Production vs dev configs and data shape | Reproduce in staging with prod data; add data quality gate |

### Symptom: Slow Job, Low CPU

| Evidence | Likely Cause | What To Check Next | Smallest Safe Fix |
| --- | --- | --- | --- |
| Stages tab shows long task durations, low CPU | Network or storage wait | Fetch wait time; S3 read latency; YARN queue wait | Investigate S3 (throttling), shuffle service, YARN policy |
| High `Fetch Wait Time` per task | Shuffle service slow or remote | Network metrics; co-located executors | Increase shuffle service capacity; investigate node distribution |
| Many tiny tasks, scheduler delay high | Over-partitioned | Partition count; AQE coalescing | Coalesce or lower shuffle partitions; enable AQE coalesce |
| High `Task Deserialization Time` | Heavy closures or PySpark serialization | Task serialization size; UDFs | Replace UDFs with built-ins; broadcast large lookups |

### Symptom: Many Output Files

| Evidence | Likely Cause | What To Check Next | Smallest Safe Fix |
| --- | --- | --- | --- |
| Final write produced many small files | Final shuffle partition count; partitionBy with high cardinality | Number of partitions feeding the write; `partitionBy` columns | Coalesce or repartition before write; reduce `partitionBy` cardinality |
| Output files larger than expected | Too few partitions feeding the write | Same | Repartition up before write |
| Output file count grows over time | Compaction not running | Lakehouse table maintenance; daily file count metric | Schedule compaction (Iceberg `rewrite_data_files`, Delta `OPTIMIZE`) |

### Symptom: Job Fails In Production But Not Dev

| Evidence | Likely Cause | What To Check Next | Smallest Safe Fix |
| --- | --- | --- | --- |
| Production data is much larger or differently shaped | Data shape change | Top keys; nulls; row count | Reproduce in staging with production-shaped data |
| Production has different IAM permissions | Permission denied on a path | Driver logs for `Access Denied`; IAM role on EMR cluster | Fix IAM; verify in staging with same role |
| Production has different dependencies | Dependency drift | Driver vs executor Python versions, JAR versions | Pin dependencies; verify in CI |
| Production runs on YARN cluster mode, dev runs in client | Driver behavior differs | Submit mode; driver logs location | Mirror production submit mode in staging |
| Production runs on Spot, dev runs on on-demand | Spot reclamation cascading failures | EMR step logs for Spot events | Move SLA-critical shuffle stages off Spot |

### Symptom: High CPU, Low IO

| Evidence | Likely Cause | What To Check Next | Smallest Safe Fix |
| --- | --- | --- | --- |
| High CPU, low shuffle, low input | UDFs or expensive expressions | UDF presence in plan; expression evaluation cost | Replace with built-ins; vectorize via Pandas UDF if necessary |
| High GC time | Memory pressure but not OOM yet | Executor memory; GC algorithm; driver vs executor heap | Increase memory or split work; tune GC |
| High serialization time | Heavy closures, broadcast variables | Closure size, broadcast bytes | Reduce closure size; use built-ins; broadcast deliberately |
| High parsing time on read | Schema inference or non-binary format | Source format; schema specification | Specify schema explicitly; switch to Parquet/ORC if possible |

## Spark UI Tabs — What Each One Tells You

| Tab | Use When | Key Evidence |
| --- | --- | --- |
| Jobs | Top-level overview; which action took how long | Job duration, status, stages per job |
| Stages | The most useful tab for diagnostics | Per-stage duration; task distribution; shuffle read/write; spill |
| Storage | Cache hits, cache memory pressure | Cached RDDs and DataFrames; memory used vs reserved |
| Environment | Verify configs that actually applied | Resolved Spark properties; classpath; environment variables |
| Executors | Executor memory pressure, GC, lost executors | Per-executor heap, GC time, failed tasks, status |
| SQL / DataFrame | The plan that actually ran | Operator graph; per-operator metrics; AQE annotations |
| Streaming Query | Structured streaming progress | Batch duration; input rate; processed rate; state metrics |

For most batch incidents, the order is **SQL → Stages → Executors**. For most streaming incidents, it is **Streaming Query → SQL → Stages → Executors**.

## Logs On EMR — Where Things Live

| Log Source | Contents | How To Find After Cluster Terminates |
| --- | --- | --- |
| Driver log | Top-level errors, plan compilation, action results | EMR step logs in S3: `s3://<emr-logs>/<cluster-id>/steps/<step-id>/stdout` and `stderr` |
| Executor logs | Task-level errors, OOM stacks, fetch failures | YARN aggregated logs in S3: `s3://<emr-logs>/<cluster-id>/containers/...` (when log aggregation is enabled) |
| YARN ResourceManager logs | Container allocation, kill reasons | EMR cluster logs in S3 |
| EMR step logs | Cluster lifecycle, bootstrap, instance fleet events | EMR console or `s3://<emr-logs>/<cluster-id>/` |
| Spark event logs | Full UI replayable in History Server | `spark.eventLog.dir=s3://.../spark-event-logs/<app-id>` |
| CloudWatch metrics | EMR cluster CPU, memory, HDFS, S3 metrics | CloudWatch console |
| S3 server access logs | S3 503/throttle counts, request volume | S3 access logging bucket (if enabled) |

The single most important production setting on an EMR Spark job is `spark.eventLog.enabled=true` with `spark.eventLog.dir=s3://...`. Without it, the cluster terminates and all evidence except aggregated YARN logs disappears. With it, the Spark History Server can rehydrate the entire UI from the S3 event log file weeks later.

## Driver vs Executor Errors — How To Tell

The first triage question on an OOM is: driver or executor?

| Signal | Driver Error | Executor Error |
| --- | --- | --- |
| Stack trace location | `org.apache.spark.SparkContext`, `org.apache.spark.sql.execution`, your application code in main thread | `org.apache.spark.executor.Executor`, `TaskRunner`, task execution code |
| Surrounding logs | Plan compilation, action results, scheduler events | Task start/finish, shuffle read/write, container heartbeats |
| In Spark UI | Executors tab → Driver row shows pressure | Executors tab → individual executor rows show pressure |
| Failure mode | Application aborts | Some tasks retry; sometimes whole stage retries; sometimes job fails |
| Common causes | `collect()`, huge plans, listing too many files | Per-task working set too large; broadcast too big; skew |
| Fix direction | Driver memory; reduce listing scope; replace `collect` with `write` | Executor memory overhead; reduce per-task working set; address skew |

Mistaking driver for executor and vice versa is one of the most common debugging errors. A `SparkException: Job aborted due to stage failure` masks an underlying `OutOfMemoryError` that could be either. Read up the stack until you see the originating thread.

## Data Issue vs Runtime Issue vs Code Issue

| Indicator | Data Issue | Runtime Issue | Code Issue |
| --- | --- | --- | --- |
| Code change since last good run | No | No | Yes |
| Data shape change since last good run | Yes | No | No |
| Cluster or config change since last good run | No | Yes | No |
| Reproducible in dev with the same input | Yes | No | Yes |
| Reproducible in dev with synthetic input | No | No | Yes |
| Plan looks identical to last good run | Yes | Yes | Often no |
| Symptom: skew, OOM on specific keys, late data | Likely | Possible | Possible |
| Symptom: container kills, executor losses, fetch failures | Possible | Likely | Less likely |
| Symptom: incorrect output, missing rows, dup rows | Likely | Less likely | Likely |

The single most useful question is "what changed?" If nothing on the code side changed and the cluster is the same, you are looking at a data issue 80% of the time.

## How To Avoid Random Config Tuning

The fastest way to make an incident longer is to start changing configs without identifying the failed operator. Some rules:

- **Change one knob at a time.** Otherwise you cannot attribute the result to the change.
- **Re-measure after every change.** "It feels faster" is not a measurement.
- **Compare the same metric.** Wall-clock time alone is noisy; look at the slow stage's duration, max task time, shuffle bytes, and spill.
- **Revert what did not help.** Configs that did nothing are technical debt for the next person.
- **Document why each surviving change is in place.** The reason should be a concrete observation in the Spark UI or a metric, not "the docs said to."

A **repeatable** pattern is to run the slow job with `spark.eventLog.enabled=true`, then use the
History Server in a sandbox to re-run plan analysis offline. You can compare two event log files
side by side, see which stage diverged, and form a hypothesis without changing production at all.

## Production Smells

- A job whose runtime varies by 5x run to run; the variance is itself the signal.
- A job whose `spark.executor.memoryOverhead` has been bumped three times and is still failing; the bottleneck is not heap.
- A job that "just needs a retry"; once a job needs a retry to succeed, the SLA is at risk.
- A "fix" that involves disabling AQE; AQE almost never makes things slower in steady state.
- A `spark.sql.shuffle.partitions=2000` override on a job that processes 100 GB; over-partitioning makes scheduler delay the bottleneck.
- A diagnosis that ends at "we added executors and it worked"; the symptom may have moved, not resolved.
- A job that runs on Spot for shuffle-heavy stages; Spot is fine for read-only compute, dangerous for shuffle.
- An on-call engineer who cannot find the slow stage in 60 seconds; that is a runbook gap, not an engineer gap.

## Best Practices For Production Debuggability

- **Persist Spark event logs to S3** for every production job. Non-negotiable.
- **Emit per-job metrics**: input rows, output rows, file counts, shuffle bytes, max task duration, max-to-median ratio.
- **Save the explain plan** for SLA-critical jobs; treat plan diffs as production change events.
- **Maintain a runbook** with the diagnostic SQL for each pipeline.
- **Mirror production submit mode in staging** (cluster mode if production uses cluster mode).
- **Pin dependencies** at the executor level; do not rely on `pip install` at startup.
- **Tag jobs** in cluster manager labels so YARN logs and CloudWatch metrics can be correlated to job names, not just IDs.
- **Treat data shape as a production change.** Add monitoring for top-key concentration, file counts, and partition row counts.

## Anti-Patterns

- Reading executor logs before opening the Spark UI.
- Tuning `spark.executor.memory` before identifying the operator that failed.
- Retrying a failed job repeatedly without preserving logs.
- Debugging a slow job using only wall-clock time.
- Concluding "data was bigger" without measuring data shape (top keys, file count, null rates).
- Assuming dev success means production safety; dev rarely has production data shape.
- Disabling AQE because of one bad rewrite; tune the thresholds instead.
- Increasing `spark.sql.shuffle.partitions` on a tiny aggregation; this just creates many small files.
- Treating a YARN container kill as "executor died for unknown reasons"; the kill reason is in the YARN log.

## Worked Example — End To End

A daily pipeline reads clickstream events, joins to campaigns, aggregates by campaign and day. The job has been stable at 25 minutes for a year. Today it took 2 hours.

Triage:

1. **What changed?** Code: no. Cluster: same EMR release, same fleet. Source data: row count up 11%; file count up 220%. Config: no diff.
2. **Slowest stage?** Stages tab shows the aggregation reduce side at 95 minutes; everything else under 10 minutes.
3. **Plan?** SQL tab shows the same plan as yesterday: scan with partition pruning, partial aggregate, exchange, final aggregate.
4. **Task distribution?** Median 8 seconds; max 41 minutes. Long tail.
5. **Data shape?** Top key today is `campaign_xyz` at 28% of rows; yesterday it was 1.2%. New campaign launch.
6. **Logs?** Two executors lost mid-stage on Spot task nodes. Container kills for memory limit.
7. **Smallest fix?** Two-phase aggregation with `salt_buckets=64` on the heavy aggregation. Verify in the SQL tab that the final plan splits the hot key. Move the heavy stage off Spot.
8. **Guardrail?** Top-key share metric, max-to-median task duration metric, alert thresholds.

Runtime returns to ~28 minutes. Two weeks later, the metric fires on a different campaign before the SLA is at risk; the on-call engineer applies the same playbook in 20 minutes.

## Real Use Case

A nightly EMR job processes clickstream and joins it to a campaign dimension table. The job was stable at 25 minutes for nine months and then quietly became 2 hours one Tuesday. Nothing in the code had changed.

The on-call engineer opened the persisted event log in the Spark History Server (the cluster had terminated hours ago). The slow stage was the aggregation, not the join. Max task time in that stage was 40× median. A profile query on the clickstream showed one new `campaign_id` had been launched and was producing ~35% of all rows for that day.

The fix was AQE skew join handling, plus two-phase aggregation on the heavy aggregation stage, plus a `top-1 key concentration` guardrail metric on the source table. The slow stage went back to ~30 minutes; the guardrail caught a similar shift two weeks later.

The lesson is the loop: mental model → evidence → smallest fix → guardrail. Everything else in this chapter is a refinement of that loop.
