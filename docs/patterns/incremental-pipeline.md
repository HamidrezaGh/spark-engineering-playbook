# Incremental Pipeline


## Problem

Full recomputation is too expensive or too slow, but the dataset changes over time. You need to process only the new/changed data while keeping outputs correct and backfillable.

## Pattern

Design the pipeline around an explicit **incremental unit** (often partition/day/hour, or a change-feed offset).

- Define a stable “incremental slice” of input (e.g., `event_date = D`, or “Kafka offsets for batch N”).
- Process only that slice.
- Write outputs in an idempotent way (overwrite/merge for that slice).
- Maintain a state/checkpoint that records what has been processed.
- Provide a backfill mode that reprocesses a bounded historical range safely.

## Tradeoffs

- Faster daily runs and lower cost, but more correctness complexity (late data, duplicates, retries).
- Requires careful output semantics (append vs overwrite vs merge).
- Requires operational discipline: checkpoints, replay logic, and backfill tooling.

## Failure Modes

- **Late-arriving data** causes undercounts unless you reprocess windows or merge changes.
- **Duplicate processing** on retries creates duplicates unless outputs are idempotent.
- **Checkpoint corruption** can cause missed or repeated slices.
- **Schema evolution** can break historical reprocessing unless versioning is planned.

## Operational Checks

- Guardrails:
  - input rows/bytes per slice
  - output rows/bytes per slice
  - file count per output partition
  - top-key concentration (skew risk)
- Persist Spark event logs for debugging incremental regressions.
- Keep a “last good run” baseline and compare daily metrics.
- Provide a safe backfill interface (range selection, throttling, stop/resume).

## Real Use Case

A daily table is built from event logs.

- Incremental unit: `event_date`.
- Daily job overwrites the partition for `event_date = D`.
- Late data: reprocess last \(N\) days each run (bounded sliding window) and overwrite those partitions.
- Backfills: run the same job for a historical date range with throttling and file-count guardrails.
