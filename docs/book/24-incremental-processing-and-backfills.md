# Incremental Processing And Backfills

## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- What “incremental” means for your pipeline (watermark/state) and what correctness risks it introduces.
- How to design backfills so they are isolated, rerunnable, and reconcilable.
- How to advance watermarks safely (only after durable success) and what late data does to you.
- What write strategies are safe for incremental updates (merge vs scoped overwrite vs append) and why.
- What operational controls you need (rate limiting, queue isolation, audits, reconciliation).

## Core Idea

Incremental processing updates only the data that changed. Backfills recompute historical ranges. Both must be designed for correctness, isolation, replayability, and operational control.

## Key Takeaways

- **Incremental processing reduces cost but increases correctness risk**.
- **Advance watermarks only after successful durable output**.
- **Backfills must be isolated, rerunnable, and reconciled**.
- **Deletes and updates require explicit source semantics and target behavior**.

## Mental Model

A high-watermark records progress, such as max event time, ingestion time, source version, or offset. It helps a job identify new input. Late-arriving records are records that belong to an older business time but arrive after the normal processing window.

Replayable pipelines can rerun a range and produce the same final state.

```text
Source changes
  -> select range by watermark
  -> transform and validate
  -> merge / scoped overwrite
  -> commit succeeded?
      |-- yes: advance watermark
      |-- no: retry same range
```

| Scenario | Safer Pattern | Risk To Control |
| --- | --- | --- |
| Daily incremental | Commit then advance watermark | Missing records |
| Late data | Lookback window or correction stream | Duplicate updates |
| Historical backfill | Isolated range and queue | Overwriting current data |
| Deletes/updates | Merge with reliable keys | Bad match semantics |

## What Spark Does Internally

Spark does not automatically know business change semantics. It processes the input you give it. Correct incremental logic depends on source metadata, keys, timestamps, merge predicates, and target write behavior.

Deletes and updates require either merge operations, delete files, tombstones, or full/partition rewrites depending on table format and design.

## Why It Matters In Production

Full reloads are simple but often become too expensive. Incremental jobs are cheaper but introduce correctness risks: missed changes, duplicate processing, late data, and complicated backfills.

## Common Failure Modes

- Watermark advances before output commit succeeds.
- Late records are ignored.
- Backfill overlaps daily job and overwrites fresh data.
- Incremental merge scans too much target data.
- Deletes are not represented in downstream tables.
- Backfill output differs from full reload.

## Design Guidance

Use:

- Durable run metadata.
- Commit-after-success watermark updates.
- Idempotent merge keys.
- Isolated backfill ranges.
- Reconciliation checks.
- Resource isolation for large backfills.
- Clear late-data policy.

Validate a backfill by comparing row counts, aggregates, duplicate keys, and business metrics against expected full-reload results.

## Operating Signals

Track:

- Watermark before and after each run.
- Input change count.
- Insert/update/delete counts.
- Late record count.
- Backfill range.
- Target partitions touched.
- Reconciliation results.

## Best Practices

- Separate event time from ingestion time.
- Make backfills rerunnable.
- Avoid advancing watermarks before durable success.
- Isolate backfill compute from daily pipelines.
- Keep source snapshots or replayable logs where possible.

## Anti-Patterns

- Using `max(event_time)` blindly as the only watermark.
- Running backfills directly against production targets without staging.
- Assuming append-only sources never send corrections.
- Ignoring deletes in source systems.
- Treating incremental output as correct without reconciliation.

## Example

```sql
MERGE INTO gold.orders t
USING staging.changed_orders s
ON t.order_id = s.order_id
WHEN MATCHED AND s.is_deleted THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

This supports inserts, updates, and deletes if the source emits a reliable change feed.

## Interview-Style Questions Covered

- How do you process only changed data?
- What is a high-watermark?
- What are late-arriving records?
- How do you design replayable Spark pipelines?
- How do you backfill one year of data without breaking current production runs?
- How do you reconcile incremental output with source-of-truth data?
- How do you handle deletes and updates in an incremental pipeline?
- How do you make backfills idempotent?
- How do you isolate backfill resources from daily production workloads?
- How do you validate that a backfill produced the same result as a full reload?

## Real Use Case

A legacy daily full reload scans 30 TB to update a customer order table. The incremental redesign consumes changed orders by ingestion watermark, writes validated staging data, merges by `order_id`, handles deletes, updates watermark only after commit, and runs historical backfills in an isolated queue with reconciliation against monthly revenue totals.
