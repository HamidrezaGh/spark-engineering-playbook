# Kafka To Iceberg Streaming


## Problem

You need to ingest Kafka topics into an Iceberg table continuously with correct semantics (exactly-once where possible, or at-least-once with idempotent writes), while managing small files, late data, schema evolution, and operational failures.

## Pattern

Use Structured Streaming with explicit operational boundaries:

- Define the ingestion contract:
  - event time vs processing time
  - watermarking and late data policy
  - keying/dedup strategy (if needed)
- Use a checkpoint location for state and offsets.
- Write to Iceberg with a strategy that avoids small files:
  - tune micro-batch interval and file sizing
  - consider periodic compaction/optimize of recent partitions
- Ensure idempotency:
  - use deterministic keys and dedup windows when at-least-once is possible
  - treat retries as normal; do not assume a batch runs only once
- Persist event logs and keep a replay/backfill path for incidents.

## Tradeoffs

- Lower latency increases small-file risk and commit overhead.
- Exactly-once is hard across multiple systems; aim for end-to-end correctness via idempotency + dedup where required.
- Stateful operations (dedup/aggregations) increase memory/state management complexity.

## Failure Modes

- Checkpoint loss/corruption causes offset confusion (replay or skip).
- Small-file explosion from tiny micro-batches.
- Late data arriving beyond watermark causes drops or correctness gaps.
- Skewed keys create state hotspots and long-tail micro-batches.
- Schema evolution breaks parsing or causes incompatible writes.

## Operational Checks

- Monitor:
  - micro-batch duration vs trigger interval (are you falling behind?)
  - input rows per batch and lag
  - output files per batch/partition
  - state store size and memory pressure (for stateful ops)
- Validate Spark UI signals for long-tail tasks, spill, and shuffle-heavy operators.
- Ensure checkpoint and table locations have correct IAM permissions and lifecycle policies.
- Provide runbooks for:
  - restarting from checkpoint
  - replaying a time range
  - compacting recent partitions after incident periods

## Real Use Case

An events topic feeds an Iceberg fact table for near-real-time analytics.

- Trigger: micro-batch every few minutes; checkpoint stored durably.
- The pipeline applies basic parsing, projection, and dedup for a bounded window.
- Output is partitioned by event date; a daily compaction rewrites recent partitions to target file sizes.
- Guardrails alert on batch lag, output file count growth, and state store growth.
