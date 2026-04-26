# Event Logs And Observability Configs

Status: Draft

## Core Idea

If you can’t inspect Spark UI after the cluster terminates, you will debug blind. Persist event logs so you can reconstruct the UI and compare runs.

## What To Enable

- Spark event logging (so you can replay UI and do regressions).
- A durable event log destination (object storage) with retention.

## UI-First Validation

- Confirm you can open historical runs and compare:
  - dominant stages and their shuffle/spill/max task time
  - SQL physical plan shape
  - executor loss / GC behavior

This section intentionally focuses on outcomes; exact config keys and storage locations differ by platform (standalone/YARN/EMR) and org standards.
