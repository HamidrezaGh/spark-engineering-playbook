# Production Debugging


## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- Where to start when a production job is slow or failing (and what evidence to gather first).
- How to locate the slowest/failed stage and read the plan + task metrics to form a hypothesis.
- How to classify “slow with low CPU” vs “slow with high spill” vs “one long tail task”.
- What the smallest safe remediation usually looks like (change one thing, re-measure).
- What guardrail/metric to add so the issue is easier next time.

## Core Idea

Production Spark debugging is evidence collection under time pressure. Start from what changed, locate the slow or failed stage, inspect data shape and physical plan, then choose the smallest safe remediation.

## Key Takeaways

- **Start from the slowest or failed stage**, not from random config changes.
- **Compare today to a known-good run**: data size, skew, files, plan, runtime, and cluster.
- **Low CPU often points to S3, shuffle wait, queue wait, or planning**, not lack of executors.
- **Every fix should add a metric or guardrail** so the same issue is easier next time.

## Mental Model

Debug Spark incidents across four layers:

- Code: plan, joins, filters, UDFs, writes.
- Data: volume, skew, schema, file count, late or duplicate records.
- Runtime: executors, memory, cores, spill, GC, YARN queues.
- Platform: S3, network, EMR capacity, IAM permissions, dependencies, EMR release.

```text
Symptom
  -> ask what changed
  -> find slow or failed stage
      |-- read physical plan
      |-- inspect task metrics
  -> form hypothesis
      |-- validate data shape
      |-- validate runtime logs
  -> apply targeted fix
  -> add metric or guardrail
```

| Symptom | First Place To Look | Common Cause |
| --- | --- | --- |
| One task much slower | Stage task distribution | Skew |
| OOM | Executor/container logs | Large partition, bad broadcast, Python overhead |
| Low CPU and slow job | Storage and shuffle wait | Object-store listing, network, queue wait |
| Many output files | Final write stage | Too many partitions or table partitions |

## What Spark Does Internally

Spark failures usually surface at the task or executor level, but root cause may be plan-level or data-level. A task OOM can be caused by skew. A slow job can be caused by small files during scan planning. A failed write can be caused by object-store commit behavior.

## Why It Matters In Production

Good triage prevents random tuning. Staff-level debugging creates a repeatable path from symptom to evidence to fix.

## Production Smells

- A job was fast yesterday and slow today.
- One task runs far longer than the rest.
- The job spills heavily to disk.
- Executors show low CPU while the job is slow.
- The output contains far more files than expected.
- Production fails but dev succeeds.

## Common Failure Modes

- OOM from skew, wide rows, bad broadcast, or too few partitions.
- Runtime regression from larger input, changed data distribution, or missing pruning.
- Small files causing slow planning and too many tasks.
- Low CPU from waiting on remote storage, shuffle fetch, or queue allocation.
- High CPU and low IO from expensive UDFs, compression, parsing, or serialization.

## Triage Flow

1. Identify what changed: code, data, config, cluster, dependencies, schedule.
2. Find the slowest or failed stage.
3. Compare input size, output size, shuffle, spill, and task distribution.
4. Read the SQL physical plan.
5. Check data profile: row counts, top keys, nulls, file counts, schema.
6. Inspect executor logs, YARN aggregated logs, EMR step logs, and CloudWatch/S3 log archives.
7. Apply a targeted fix.
8. Add a metric or guardrail to prevent recurrence.

## Spark UI Signals

Use:

- SQL tab for physical operators.
- Stages tab for task skew, shuffle, spill, failures.
- Executors tab for GC, lost executors, memory pressure.
- Storage tab for cache behavior.
- Event logs for after-the-fact analysis.

## Best Practices

- Keep historical run metrics for comparison.
- Emit input rows, output rows, file count, shuffle bytes, spill bytes, and runtime.
- Save explain plans for critical jobs.
- Maintain a failure triage checklist.
- Treat data distribution changes as production changes.
- Preserve Spark event logs to S3 so EMR jobs can be debugged after cluster termination.
- Correlate Spark stage regressions with S3 request errors, EMR instance health, and YARN queue pressure.

## Anti-Patterns

- Changing executor memory before finding the failed operator.
- Retrying failed jobs without preserving logs.
- Debugging only from application logs and ignoring Spark UI.
- Assuming dev success proves production safety.

## Example

A job reads 10 TB but outputs 10 GB. Before adding executors, check whether filters are pushed down, whether partition pruning works, whether required columns are pruned, and whether the source table layout matches the query pattern.

## Interview-Style Questions Covered

- A Spark job was fast yesterday but slow today. How do you debug?
- A job fails with OOM. What do you check first?
- A join stage has one task running for 40 minutes while others finish in 2 minutes. What is likely happening?
- A job creates 50,000 small Parquet files. Why?
- A job reads 10 TB but outputs only 10 GB. How would you optimize?
- A job spills heavily to disk. What options do you have?
- A job is slow but no executor is using much CPU. What could be wrong?
- A job has high CPU but low IO. What could be happening?
- A Spark job fails only in production, not dev. Why?
- How would you create a checklist for Spark job failure triage?

## Real Use Case

A job regresses from 20 minutes to 2 hours. The code did not change, but the source table received a large late-arriving partition and one merchant key now dominates the data. The Spark UI shows one skewed shuffle task and heavy spill. The fix combines data profiling, skew handling for the hot merchant, and an alert on top-key concentration.
