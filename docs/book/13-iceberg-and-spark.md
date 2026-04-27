# Iceberg And Spark

## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- What Iceberg adds vs “a directory of Parquet files” (snapshots, metadata, atomic commits).
- How snapshot/manifest metadata drives planning and pruning.
- Why write patterns like `MERGE INTO` can be expensive (files touched, rewrites, metadata churn).
- What to check first when Iceberg performance regresses (planning time, file counts, partitioning, metadata).
- What maintenance tasks keep tables healthy (compaction, rewrite manifests, snapshot expiration).

## Core Idea

Apache Iceberg is a table format that adds transactional table metadata on top of data files. Spark writes data files and commits metadata changes so readers see consistent snapshots.

## Key Takeaways

- **Iceberg table state is metadata-driven**, not just a directory of files.
- **Snapshots enable isolation, rollback, and time travel**.
- **Manifests and manifest lists drive planning and pruning**.
- **Large `MERGE INTO` operations are expensive when many target files are touched**.

## Mental Model

Iceberg tables are made of snapshots. A snapshot points to manifest lists, which point to manifests, which track data files and delete files. This metadata layer lets Iceberg support snapshot isolation, time travel, schema evolution, partition evolution, and hidden partitioning.

```text
Iceberg table
  -> current snapshot
      -> manifest list
          |-- manifest A
          |     |-- data files
          |     |-- delete files
          |
          |-- manifest B
                |-- data files
                |-- delete files
```

| Metadata Layer | What It Tracks | Why It Matters |
| --- | --- | --- |
| Snapshot | A committed table state | Isolation, rollback, time travel |
| Manifest list | Manifests in a snapshot | Planning entry point |
| Manifest | Data/delete file metadata | File pruning and scan planning |
| Data file | Actual records | Read and write cost |

## What Spark Does Internally

When Spark writes to Iceberg, tasks write data files and Spark commits a new table snapshot through the Iceberg catalog. Readers use the current snapshot metadata to plan which files to scan.

Hidden partitioning lets users query logical columns while Iceberg applies partition transforms such as day, month, bucket, or truncate. This avoids many Hive-style problems where users must know physical partition columns.

`MERGE INTO` rewrites affected data files or writes delete files depending on table configuration, operation type, and engine behavior. It can be expensive because matching records may require scanning and rewriting many files.

## Why It Matters In Production

Iceberg improves correctness and operability compared with raw file writes, but it introduces metadata maintenance responsibilities:

- Manifest growth.
- Small files.
- Snapshot expiration.
- Delete file accumulation.
- Compaction.
- Catalog reliability.

## Common Failure Modes

- Large merge scans too many files.
- Too many small files and manifests slow planning.
- Partition spec does not match query or merge predicates.
- Snapshot retention keeps too much metadata.
- Concurrent writes conflict and require retry.

## Tuning And Configuration

Optimize large merges by:

- Filtering source and target aggressively.
- Designing partition specs around common merge predicates.
- Clustering or sorting data by merge keys where useful.
- Compacting small files.
- Managing delete files.
- Running manifest rewrite and snapshot expiration maintenance.

## Spark UI Signals

Look for:

- Target table scan size during merge.
- Join strategy inside merge plans.
- Number of files touched.
- Write file counts.
- Planning time.
- Commit failures or retries.

## Best Practices

- Use Iceberg table operations instead of raw path overwrites.
- Design partition specs for pruning, not just directory readability.
- Schedule compaction and metadata maintenance.
- Use snapshot time travel for recovery and audit.
- Monitor file and manifest counts.

## Anti-Patterns

- Treating Iceberg as "just Parquet files."
- Running huge unfiltered merges.
- Ignoring delete file buildup.
- Partitioning by high-cardinality raw columns without considering transforms.
- Expiring snapshots without understanding recovery requirements.

## Example

```sql
MERGE INTO prod.customer_profile t
USING staging.customer_profile_updates s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

This is simple syntax, but the production cost depends on how many target files must be scanned and rewritten.

## Self-check (concept review)

- How does Spark write to an Iceberg table?
- What is the difference between Spark partitioning and Iceberg hidden partitioning?
- What is Iceberg metadata?
- What are manifests and manifest lists?
- What is snapshot isolation in Iceberg?
- How does Iceberg support time travel?
- Why does Iceberg avoid Hive-style partition problems?
- What is `MERGE INTO` in Iceberg?
- Why can `MERGE INTO` be expensive?
- How would you optimize a large Iceberg merge?

## Real Use Case

A customer profile table receives hourly updates. A naive merge scans most of the table and rewrites thousands of files. The fix is to partition or cluster around update access patterns, filter updates by changed date ranges, compact small files, monitor manifest growth, and use snapshot rollback as the recovery mechanism for bad writes.
