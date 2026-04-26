# Spark Production Readiness Review Template

Use this template before promoting a Spark job from staging to production, or before adopting an existing job into a managed on-call rotation. The intent is to make sure the job is something the on-call engineer can debug at 2 AM without paging the original author.

A job that is not production-ready usually has at least one of the following: no idempotency, no replayability, no schema evolution plan, no metrics, no alerts, no owner, or no runbook. This template asks each question explicitly.

## Job Identity

- **Job name**:
- **Owning team**:
- **On-call rotation**:
- **Code repository / path**:
- **Runbook URL**:

## Idempotency

- Can the job be run twice with the same input and produce the same output?
- If the job partially succeeds, is the partial state safe to recover from?
- Are output writes idempotent? (overwrite, merge, atomic commit)
- Are output side effects idempotent? (notifications, downstream triggers)
- If idempotency is impossible, what is the compensating mechanism?

## Replayability

- Can the job be re-run for an arbitrary past time window?
- Is the input source replayable (Kafka with retention, partitioned table, audit log)?
- Are derived inputs (lookups, dimensions) versioned or point-in-time queryable?
- Is there a replay test (e.g., re-run yesterday's run with the same input and compare outputs)?

## Backfill Safety

- Can the job be backfilled for the last 30 days? 90 days? 1 year?
- Is the backfill bounded (slice-based)?
- Is there a concurrency limit on backfill?
- Does backfill produce the same output shape as a regular run?
- What happens to downstream consumers during a backfill?

## Schema Evolution

- What is the schema contract for the input?
- What is the schema contract for the output?
- What happens if a new column is added upstream?
- What happens if a column is removed upstream?
- What happens if a column type changes?
- Is the schema validated at job start, or only when a row is processed?

## Data Quality Checks

- What invariants must hold on the input? (non-null keys, valid date ranges, expected row counts)
- What invariants must hold on the output? (row count vs input, no duplicate keys, sum totals)
- Where do these checks live? (in-job assertions, post-job dq tool, downstream watch)
- What happens when a check fails? (fail the job, mark output dirty, alert, ignore)

## Metrics

The job should emit, at minimum:

- Input rows / input bytes
- Output rows / output bytes
- File count read and written
- Per-stage shuffle bytes for the heaviest stage
- Max task duration / max-to-median task ratio for the heaviest stage
- Total runtime
- Cluster cost (if available)

For streaming jobs, additionally:

- Batch duration trend
- State store size per operator
- Watermark lag from real time
- Input rate / processed rate

- Where are the metrics stored?
- Where are the dashboards?
- What is the retention?

## Logs

- Are application logs persisted?
- Are Spark event logs persisted to durable storage? (`spark.eventLog.enabled=true`, `spark.eventLog.dir=s3://...`)
- How long are logs retained?
- Can the on-call engineer find logs for a specific run id?

## Alerting

- What alerts fire on what conditions?
- For each alert: who is paged?
- For each alert: is there a runbook link in the alert payload?
- For each alert: what is the expected response time?
- Are there hygiene alerts (slow drift) in addition to outage alerts?

## Ownership

- Who owns this job operationally?
- Who owns the underlying business logic?
- What is the escalation path if the on-call cannot resolve the issue?
- Where is ownership documented and is it current?

## Runbook

- Does a runbook exist?
- Does it cover: how to start the job, how to stop the job, how to back-fill, how to reprocess a single partition?
- Does it cover: common failure modes and the diagnostic path for each?
- Does it cover: emergency contacts, escalation, and known good states?
- Has it been used recently? (a runbook nobody reads is wrong; a runbook used during an incident is up to date)

## Cluster And Resources

- What cluster shape does the job run on?
- Is the sizing documented and justified?
- Is the job using Spot, on-demand, or a mix?
- Are SLA-critical shuffle stages on stable capacity?
- Is there auto-scaling? Is it configured to fail safely?

## Dependencies

- Python: are dependencies pinned?
- JAR: are versions pinned?
- EMR bootstrap actions: are they versioned?
- Are dependencies the same in staging and production?
- Is there a CI job that catches dependency drift?

## Security And Permissions

- What IAM role does the job use?
- Is the role least-privilege? (only the buckets, tables, secrets it needs)
- Where are secrets stored? (Secrets Manager, KMS, etc.)
- Is data at rest encrypted? With which key?
- Does the role have any recent CloudTrail anomalies?

## Reviewer Sign-Off

- [ ] Job is idempotent and replayable.
- [ ] Backfill is bounded and safe.
- [ ] Schema evolution is handled or documented as out-of-scope.
- [ ] Data quality checks are in place.
- [ ] Metrics are emitted, stored, and visualized.
- [ ] Logs are persisted long enough to debug week-old incidents.
- [ ] Alerts are tied to runbooks; pages have links.
- [ ] Ownership is unambiguous.
- [ ] Runbook covers common failures.
- [ ] Cluster and resource decisions are documented.
- [ ] Dependencies are pinned and consistent across environments.
- [ ] IAM and secrets are least-privilege and current.

Reviewer name and date:
