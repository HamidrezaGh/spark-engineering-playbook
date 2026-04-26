# Structured Streaming

Status: First Draft
Level: Senior to Staff
Covers: micro-batches, checkpoints, watermarks, stateful processing, fault tolerance, Kafka, Iceberg

## Core Idea

Structured Streaming treats streaming data as an unbounded table. Spark runs the query continuously, usually as a sequence of micro-batches, and uses checkpoints to track progress and recover from failures.

## Mental Model

A micro-batch processes a bounded slice of new data. Checkpoints store offsets, progress, and state metadata. Stateful operations such as aggregations, deduplication, joins, and session windows maintain state across micro-batches.

Watermarking tells Spark how long to wait for late data before old state can be dropped.

```text
Kafka/source offsets
  -> micro-batch
      |-- update state store
      |-- write to sink
      |-- write checkpoint: offsets + progress + state metadata

restart
  -> load checkpoint
  -> resume from stored offsets and state
```

| Concept | What It Protects | Failure If Missing |
| --- | --- | --- |
| Checkpoint | Progress and state recovery | Reprocessing or inability to resume |
| Watermark | Bounded state for late data | Unbounded state or dropped valid data |
| Idempotent sink | Retry safety | Duplicate output |
| Trigger interval | Latency and batch size | Tiny files or growing lag |

## What Spark Does Internally

For Kafka, Spark records consumed offsets in the checkpoint. For stateful operations, Spark stores state in a state store and updates it per batch. On restart, Spark reloads checkpoint information and resumes from known offsets and state.

If checkpoint data is deleted, Spark loses its streaming memory: offsets, state, and progress. The query may reprocess data, fail to resume, or produce incorrect results depending on source and sink.

## Why It Matters In Production

Exactly-once is not automatically end-to-end. Spark can provide strong processing guarantees with replayable sources and idempotent or transactional sinks, but the full pipeline depends on source semantics, sink semantics, checkpoint integrity, and write design.

Writing Kafka data to Iceberg safely requires checkpointing, deterministic transformations, a stable target table, and careful handling of retries, schema evolution, and small files.

## Common Failure Modes

- Deleted checkpoints cause reprocessing or unrecoverable state loss.
- No watermark causes unbounded state growth.
- Watermark too aggressive drops valid late data.
- Small files accumulate from frequent micro-batches.
- Sink is not idempotent, so retries duplicate output.
- Kafka offsets advance without a safe write strategy.

## Tuning And Configuration

Tune:

- Trigger interval.
- Input rate limits.
- State store size.
- Watermark delay.
- Shuffle partitions.
- Output file compaction strategy.
- Checkpoint storage reliability.

For low-latency pipelines, tune for stable batch duration below trigger interval. For throughput pipelines, tune for larger batches and efficient file sizes.

## Spark UI Signals

Track:

- Batch duration.
- Input rows per second.
- Processed rows per second.
- State rows and state memory.
- Watermark progress.
- Sink write time.
- Failed batches and restart behavior.

## Best Practices

- Store checkpoints in durable storage.
- Never casually delete checkpoints in production.
- Use watermarks for stateful event-time processing.
- Make sinks idempotent or transactional.
- Monitor state growth.
- Plan compaction for file sinks and lakehouse tables.

## Anti-Patterns

- Calling a streaming pipeline exactly-once without validating sink semantics.
- Using tiny trigger intervals for object-store table writes without compaction.
- Changing query state schema without a migration plan.
- Sharing checkpoints between unrelated queries.

## Example

```python
query = (
    spark.readStream.format("kafka")
         .option("subscribe", "events")
         .load()
         .writeStream
         .format("iceberg")
         .option("checkpointLocation", "s3://checkpoints/events_to_iceberg/")
         .toTable("prod.events")
)
```

This skeleton still needs production decisions for schema parsing, idempotency, trigger interval, table maintenance, and failure recovery.

## Interview-Style Questions Covered

- What is the difference between batch Spark and Structured Streaming?
- What is a micro-batch?
- What is checkpointing?
- What is watermarking?
- What is stateful processing?
- What happens if checkpoint data is deleted?
- How does Spark guarantee fault tolerance in streaming?
- What is exactly-once processing?
- Is exactly-once always truly exactly-once end-to-end?
- How would you write Kafka data to Iceberg safely?

## Real Use Case

A product analytics platform streams Kafka events into Iceberg every minute. The first version creates thousands of tiny files and has no late-data policy. The production design increases trigger interval, adds checkpoint monitoring, uses watermarks for aggregations, schedules compaction, and validates that retries do not duplicate committed Iceberg snapshots.
