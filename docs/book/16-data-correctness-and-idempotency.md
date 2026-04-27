# Data Correctness And Idempotency

## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- What “idempotent” means for a Spark pipeline (safe reruns, no duplicates/partial output).
- Which write modes are safest for retryable pipelines (and what can go wrong with append/overwrite).
- Where correctness usually breaks in production (late data, retries, partial commits, backfills).
- What quality gates and run metadata you need to publish “gold” data safely.
- How to design reruns/backfills so they are operationally safe and observable.

## Core Idea

A Spark job is idempotent when rerunning it with the same inputs produces the same correct final state without duplicates, missing records, or corrupt partial output. Production pipelines must assume retries, partial failures, late data, and backfills will happen.

## Key Takeaways

- **Idempotency means safe reruns**, not just successful first runs.
- **Append is risky for retryable jobs** unless the input and sink semantics prevent duplicates.
- **Quality gates should run before publishing gold data**.
- **Watermarks and run metadata must advance only after durable success**.

## Mental Model

Correctness is a contract across input, transformation, and write path. Spark can retry tasks, but business correctness depends on deterministic logic and safe sink behavior.

Append, overwrite, dynamic partition overwrite, and merge have different risk profiles:

- Append adds new data and can duplicate records if rerun unsafely.
- Overwrite replaces a target and can destroy good data if scoped incorrectly.
- Dynamic partition overwrite replaces only touched partitions but still needs correct partition scope.
- Merge updates/inserts matching keys but can be expensive and depends on key quality.

```text
Input range or source snapshot
  -> validated staging data
  -> quality gate
      |-- fail: stop before gold
      |-- pass: atomic publish or merge
              -> run metadata and watermark
              -> reconciliation checks
```

| Write Pattern | Retry Risk | Safer When |
| --- | --- | --- |
| Append | Duplicate records | Input is exactly-once and immutable |
| Overwrite | Data loss if scope is wrong | Target scope is small and validated |
| Dynamic partition overwrite | Wrong partitions replaced | Touched partitions are explicit |
| Merge | Bad keys corrupt matches | Keys and source uniqueness are validated |

## What Spark Does Internally

Spark tasks may be retried. Write jobs may create temporary files before commit. If the application fails mid-write, some sinks can leave partial or orphan files unless the table format or commit protocol handles atomicity.

Table formats such as Iceberg and Delta provide stronger commit semantics than raw path writes because readers see committed snapshots rather than arbitrary files in a directory.

## Why It Matters In Production

Most serious data incidents are correctness incidents, not speed incidents. A fast job that duplicates revenue or deletes partitions is worse than a slow job.

## Common Failure Modes

- Retried append duplicates data.
- Failed overwrite leaves partial output.
- Backfill overlaps daily job and writes conflicting data.
- Schema drift silently shifts fields.
- Source sends duplicate or late records.
- Multi-table writes leave inconsistent downstream state.

## Tuning And Configuration

Correctness tuning is about design choices:

- Use deterministic primary or natural keys where possible.
- Write to transactional tables for critical data.
- Scope overwrites by partition or predicate.
- Validate before publishing.
- Separate staging and production tables.
- Use snapshot rollback where available.

## Operational Signals

Track:

- Input/output row counts.
- Duplicate key counts.
- Null rates for required columns.
- Schema versions.
- Partition counts touched.
- Merge matched/updated/inserted counts.
- Reconciliation against source-of-truth totals.

## Best Practices

- Make every production write retry-safe.
- Add data quality gates before gold tables.
- Design backfills to be isolated and replayable.
- Store run metadata with input ranges and code version.
- Prefer atomic table commits over raw directory mutation.

## Anti-Patterns

- Appending blindly in rerunnable jobs.
- Overwriting full tables for small corrections.
- Treating row count equality as the only quality check.
- Ignoring duplicate keys before merge.
- Writing multiple output tables without a recovery plan.

## Example

```sql
MERGE INTO gold.customer_daily t
USING staging.customer_daily_run s
ON t.customer_id = s.customer_id AND t.event_date = s.event_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

This is safer than append for reruns if the merge key is correct and staging data is validated.

## Interview-Style Questions Covered

- What does it mean for a Spark job to be idempotent?
- How do you safely retry a failed Spark write?
- How do you prevent partial output from corrupting downstream tables?
- What is the difference between append, overwrite, dynamic partition overwrite, and merge?
- How do you design a backfill so it does not duplicate or lose data?
- How do you validate row counts, null rates, duplicate keys, and business invariants?
- How do you handle schema drift in production?
- How do you design a data quality gate before publishing a table?
- How do you reconcile Spark output with a source-of-truth system?
- How do you make failures safe when a pipeline writes multiple tables?

## Real Use Case

A billing pipeline recomputes daily invoices. Appending reruns creates duplicate invoices. A sound design writes each run to staging, validates totals against source transactions, merges into the gold invoice table by invoice ID and billing date, and records the source watermark and snapshot ID for audit.
