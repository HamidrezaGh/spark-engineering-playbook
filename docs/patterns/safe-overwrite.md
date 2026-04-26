# Safe Overwrite

## Problem

You need to overwrite a table (or a table partition) safely in production without leaving partial output, corrupting readers, or creating duplicates on retries.

## Pattern

Use an overwrite strategy that is **atomic at the reader level** and **idempotent** at the job level:

- Prefer partition-scoped overwrites when possible (overwrite only the affected partitions/slices).
- Write to a temporary/staging location or use a table format that supports atomic commits.
- Commit only after all tasks succeed.
- Make reruns safe: the same run can be executed twice without duplicating data.

## Tradeoffs

- Safer correctness but can be more expensive than pure append.
- Partition overwrites are cheaper than full-table overwrites, but require good partition design.
- Atomic commit semantics depend on the table format and commit protocol.

## Failure Modes

- Partial outputs visible to readers if the commit is not atomic.
- Concurrent writers overwrite each other without coordination.
- Object-store commit protocol mismatches can cause orphaned files or inconsistent metadata.
- Overwriting too many partitions can create a small-file explosion and metadata growth.

## Operational Checks

- Confirm the overwrite scope (which partitions/slices) before running.
- Validate idempotency: reruns produce the same logical output.
- Monitor:
  - output file count and size distribution
  - commit time and failure rate
  - orphaned/staging file cleanup
- For critical tables, require a rollback strategy (restore snapshot / rebuild partitions).

## Real Use Case

A daily aggregate table is recomputed for `event_date = D` when late data arrives.

- The job overwrites only the affected partition(s), not the whole table.
- The write uses atomic commit semantics from the table format (or a staging+commit workflow).
- A guardrail alerts if the overwrite produces an abnormal number of files for that partition.
