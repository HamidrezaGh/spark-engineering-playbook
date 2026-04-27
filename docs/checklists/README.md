# Checklists

Checklists turn the handbook into operating standards. They should be short, reviewable, and useful in pull requests, incident response, and production readiness reviews.

## Index

| Checklist | When to use it |
| --- | --- |
| [Spark job design](spark-job-design-checklist.md) | New pipeline: shuffles, joins, writes, and ops contract. |
| [Spark debugging](spark-debugging-checklist.md) | First 15 minutes in the Spark UI. |
| [Spark SQL review](spark-sql-review-checklist.md) | PR review of SQL / DataFrame read and join path. |
| [Spark write path](spark-write-path-checklist.md) | Before turning on a batch write to a lake or warehouse. |
| [Spark + Iceberg](spark-iceberg-checklist.md) | Merge, snapshot, and compaction for Iceberg tables. |
| [Spark on EMR / YARN](spark-emr-checklist.md) | Cluster mode, memory, Spot, and logging. |
| [Spark streaming](spark-streaming-checklist.md) | Structured Streaming checkpoints, watermarks, sinks. |
| [Production Readiness](production-readiness.md) | Before declaring a pipeline or cluster change production-grade. |
| [Job Failure Triage](job-failure-triage.md) | First hour of an incident: scope, Spark UI, logs, smallest fix. |
| [Pre-Deploy Review](pre-deploy-review.md) | PR or release gate for a Spark job change. |
| [Cost Review](cost-review.md) | Periodic or reactive review of runtime, shuffle, storage, and spend. |
