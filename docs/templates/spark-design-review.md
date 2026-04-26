# Spark Design Review Template

Use this template before launching a new Spark job in production, before significantly changing an existing job's shape (input volume, partitioning, output destination), or before approving a pull request that introduces a new pipeline.

The reviewer's job is to make sure each section has a concrete, evidence-based answer. "We will figure that out in production" is not a valid answer to most of these questions.

## Job Identity

- **Job name**:
- **Owning team / on-call rotation**:
- **Runtime environment** (EMR release, Databricks runtime, Kubernetes, etc.):
- **Trigger** (cron, event-driven, streaming):
- **Linked design doc / ticket**:

## Workload Shape

- What does the job do, in one sentence?
- What is the input volume per run? (rows, bytes, files)
- What is the output volume per run? (rows, bytes, files)
- What is the expected runtime?
- Is this batch, streaming, or hybrid?
- What is the typical shape of a single record (wide row vs narrow row)?
- What is the cardinality of any natural keys (customer, account, merchant, tenant)?

## SLA / Freshness Requirement

- What is the end-to-end SLA?
- Who consumes the output, and when?
- What happens if the SLA is missed by 1 hour? By 6 hours? By 24 hours?
- Is the SLA measured and alerted on?

## Input Size And Output Size

- What is the source table or topic?
- Is the source table partitioned? On what column(s)?
- What is the typical input bytes per partition?
- How will the job read the input — full scan, partition filter, time window?
- What is the destination table or sink?
- What is the typical output bytes per run?
- Is the destination partitioned? On what column(s)?

## Shuffle Risk

- How many shuffles will the job perform? (count `Exchange` nodes in `EXPLAIN FORMATTED`)
- What is the expected shuffle volume per shuffle?
- What is `spark.sql.shuffle.partitions` for this job? Why?
- Is AQE enabled? Are coalesce and skew join handling enabled?
- What is the maximum acceptable shuffle bytes per stage?

## Join Risk

- What joins does the job perform?
- For each join: which strategy is expected (broadcast / sort-merge / shuffled hash)? On what evidence?
- For each join: are filters and projections pushed before the join?
- For each join: are the join keys clean, well-typed, and non-null where expected?
- For each broadcast join: is there a size guardrail on the build side?
- Are statistics (row count, column stats) up to date on the joined tables?

## Skew Risk

- What is the top-key concentration on each shuffle key?
- Is there a known hot key? How is it handled?
- If salting is used: is the second aggregation present?
- If hot-key isolation is used: where is the hot-key list maintained?
- Is the heavy stage running on Spot capacity, or on-demand?

## Storage Risk

- What file format is the output (Parquet, ORC, Iceberg, Delta)?
- What is the expected number of output files per run? Per partition?
- What is the target file size?
- Is there a compaction or table-maintenance schedule?
- What is the snapshot expiration / retention policy (for lakehouse tables)?

## File Count Risk

- What is the expected files-per-partition for the destination?
- Will the job produce >200 files per partition? If yes, is that intentional?
- What is the producer-side file count metric? Where is it surfaced?

## Failure Recovery

- What happens if the job fails halfway?
- Is the output idempotent? (re-running the same job with the same input produces the same output)
- Is there a safe rerun path?
- Where are Spark event logs persisted? (`spark.eventLog.dir`)
- Where are application/driver logs persisted?

## Backfill Strategy

- How would you rerun this job for a single past partition?
- How would you rerun this job for a year of past data?
- Is the backfill bounded (slice-based, with checkpointing) or unbounded?
- What is the concurrency limit for backfill?
- What guardrails block backfill from corrupting downstream data?

## Observability

- What metrics does the job emit?
- At minimum: input rows, output rows, file count, shuffle bytes, max task duration, max-to-median task ratio, runtime.
- Where are these metrics stored?
- What dashboards display them?
- What alerts fire on what conditions?

## Cost Controls

- What is the expected cluster cost per run?
- What is the cluster sizing? Why?
- Is the job using Spot, on-demand, or a mix?
- Are SLA-critical shuffle stages on stable (on-demand) capacity?
- What is the trailing 30-day cost trend?

## Rollback Strategy

- If this PR causes a production regression, how do you roll it back?
- Is the change behind a feature flag, a config, or a code revert?
- How fast can rollback be executed?
- Does rollback require reprocessing data?

## Reviewer Sign-Off

- [ ] Workload shape is documented and matches the SLA.
- [ ] Shuffle volumes are bounded and validated against `EXPLAIN FORMATTED`.
- [ ] Join strategies are intentional and have size guardrails where applicable.
- [ ] Skew risk is acknowledged and mitigated.
- [ ] Storage and file-count risks are bounded.
- [ ] Backfill plan exists and is operationally safe.
- [ ] Observability is in place.
- [ ] Cost is in line with the workload value.
- [ ] Rollback is fast and tested.

Reviewer name and date:
