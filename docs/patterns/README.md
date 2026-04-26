# Patterns

Patterns describe reusable production designs. Each pattern should include when to use it, when to avoid it, architecture, failure modes, operational checks, and a real use case.

## Index

| Pattern | Focus |
| --- | --- |
| [Incremental Pipeline](incremental-pipeline.md) | Bounded daily or hourly runs with clear watermarks and idempotent publishes. |
| [Safe Overwrite](safe-overwrite.md) | Overwrite and dynamic-partition semantics without accidental data loss. |
| [Large Iceberg Merge](large-iceberg-merge.md) | Scoped `MERGE` / upsert jobs on big tables without runaway shuffle. |
| [Kafka To Iceberg Streaming](kafka-to-iceberg-streaming.md) | Durable streaming ingest with checkpointing and table commit discipline. |
| [Idempotent Backfill](idempotent-backfill.md) | Replayable historical loads that do not double-write or corrupt gold data. |
