# Idempotent Backfill

Status: Draft

## Problem

You need to recompute historical data (days/weeks/months) without creating duplicates, corrupting tables, or generating inconsistent results when the backfill is retried, paused, or rerun.

## Pattern

Make backfills **slice-based** and **idempotent**:

- Choose a deterministic backfill unit (partition/day/hour or a key-range).
- For each slice:
  - read the exact input slice
  - compute outputs deterministically
  - write outputs using safe overwrite/merge semantics for that slice
- Track progress (checkpoint/state) so you can resume safely.
- Throttle concurrency so you don’t overload S3/YARN or create extreme shuffle contention.

## Tradeoffs

- Safer correctness but more operational plumbing (state, retries, orchestration).
- Overwriting/merging is often more expensive than pure append, but avoids duplicates.
- Backfills can stress metadata and small-file behavior if not controlled.

## Failure Modes

- Duplicate data on retries if outputs are append-only.
- Partial backfill leaves mixed versions of history.
- Unbounded backfill creates small files and causes downstream planning regressions.
- Backfill changes data distribution and triggers skew/OOM in stages that were stable in daily mode.

## Operational Checks

- Guardrails:
  - file count budget per partition/day
  - shuffle/spill budget for the heaviest stage
  - max runtime and concurrency limits
- Ensure event logs are persisted for post-mortem analysis.
- Validate correctness invariants after each slice (row counts, uniqueness, checksums where appropriate).
- Provide an emergency stop and a safe resume path.

## Real Use Case

A pipeline needs to rebuild 180 days of a fact table after a logic bug fix.

- Backfill unit: `event_date`.
- The job overwrites one day at a time and records progress.
- Concurrency is capped to avoid saturating S3 and to control shuffle pressure.
- A guardrail blocks the backfill if any day exceeds a file-count budget or if skew metrics exceed thresholds.
