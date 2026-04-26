# Production Readiness Checklist

## Data Correctness

- Idempotent outputs: reruns and retries don’t duplicate or corrupt data.
- Clear semantics: append vs overwrite vs merge are intentional and documented.
- Late and duplicate data handling is defined (especially for streaming/incremental jobs).
- Schema evolution plan: compatibility, backfill strategy, and downstream impact.
- Data quality checks: basic invariants (row counts, null rates, uniqueness where required).

## Performance

- Expected scale is documented (input bytes/rows, output bytes/rows).
- Known expensive operators are identified (shuffles, joins, sorts, windows).
- Partition sizing strategy is defined (avoid too few huge tasks or too many tiny tasks).
- Output file sizing expectations are defined (avoid small-file explosion).
- A baseline run exists for comparison and regression detection.

## Reliability

- Retries are safe (idempotency) and bounded (avoid infinite retry storms).
- Failure modes are documented (OOM, skew, fetch failures, write failures) with playbooks.
- Dependencies are versioned and reproducible.
- Cluster/workload isolation needs are understood (queue, concurrency, noisy neighbor risk).

## Observability

- Event logs are persisted for post-run debugging.
- Metrics emitted: input/output rows, file counts, runtime, shuffle bytes, spill bytes, max task duration.
- Logs include enough context to identify the failing partition/key/day.
- Alerts exist for SLA breaches and for guardrail metrics (file count, skew, spill).

## Operations

- Clear on-call/runbook ownership and escalation path.
- Backfill procedure is defined and safe (throttling, partition selection, stop/resume).
- Rollback strategy exists (restore snapshot, revert version, rebuild partitions).
- Cost budget is understood (compute hours, S3 request costs, storage growth).
