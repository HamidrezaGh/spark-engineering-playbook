# Object Storage With Spark

Status: First Draft
Level: Senior to Staff
Covers: S3, GCS, ADLS, list operations, rename, committers, throttling, metadata operations

## Core Idea

Object storage is not a filesystem. It is highly scalable storage with different semantics and performance characteristics. Spark jobs must account for list cost, request rates, commit behavior, object metadata, and non-atomic rename patterns.

## Mental Model

HDFS is built for distributed filesystem operations. Object stores such as S3, GCS, and ADLS expose object APIs. Operations such as listing many prefixes, writing many small objects, and renaming paths can be expensive or implemented as copy/delete workflows.

```text
Filesystem intuition:
rename(pathA, pathB) -> cheap metadata operation

Object-store reality:
rename-like behavior -> copy object(s) + delete old object(s)
listing many prefixes -> many remote metadata requests
many tiny files -> many remote reads and request charges
```

| Operation | HDFS Intuition | Object Storage Reality |
| --- | --- | --- |
| Rename | Cheap and atomic | Often copy/delete and expensive |
| List directory | Metadata lookup | Remote API calls over many objects |
| Write many files | Mostly local cluster concern | Request cost, commit cost, planning cost |
| Read partitioned table | Directory pruning | Depends on metadata, listings, and layout |

## What Spark Does Internally

Spark scans object storage by listing paths and planning file reads. Many small files mean many object metadata operations and many tiny tasks. During writes, commit protocols must make task outputs visible safely even though object-store rename may not be atomic or cheap.

Table formats reduce risk by committing metadata snapshots rather than relying only on directory state.

## Why It Matters In Production

Object storage bottlenecks often look like Spark slowness:

- Low CPU utilization.
- Slow scan planning.
- High task startup overhead.
- Throttling or request rate errors.
- Slow commit phases.
- Expensive small-file workloads.

## Common Failure Modes

- S3 throttling from too many concurrent requests.
- Slow directory listing over many partitions and files.
- Rename-heavy commit protocol slows writes.
- Orphan files after failed jobs.
- Cost spikes from metadata-heavy workloads.

## Tuning And Configuration

Tune object-storage workloads by:

- Reducing small files.
- Avoiding unnecessary listings.
- Using table metadata for pruning.
- Choosing appropriate committers.
- Controlling write parallelism.
- Compacting data files.
- Designing partition layouts that do not create too many prefixes or files.

## Operating Signals

Monitor:

- Object-store request count and throttling.
- File count scanned.
- Listing time.
- Read/write throughput.
- Commit time.
- Average file size.
- Retry counts from storage clients.

## Best Practices

- Use transactional table formats for important datasets.
- Keep files reasonably sized.
- Use partition pruning and metadata pruning.
- Avoid treating object storage as POSIX storage.
- Separate storage bottlenecks from Spark compute bottlenecks during debugging.

## Anti-Patterns

- Writing thousands of tiny objects per batch.
- Depending on directory listing as the source of truth for table state.
- Using high-cardinality path partitioning.
- Assuming rename is atomic and cheap everywhere.

## Example

```python
spark.read.parquet("s3://lake/events/").where("event_date = '2026-04-25'")
```

This is efficient only if the table layout and metadata let Spark avoid listing and scanning irrelevant files.

## Interview-Style Questions Covered

- Why is S3, GCS, or ADLS not the same as a filesystem?
- Why are rename operations expensive or unsafe on object stores?
- How do list operations affect Spark planning and runtime?
- How do small files affect object-store cost and latency?
- What are common symptoms of S3 throttling?
- How do you tune Spark for object-storage-heavy workloads?
- How do committers reduce object-store write problems?
- How do table formats like Iceberg, Delta, and Hudi reduce object-store risks?
- How do you design a pipeline to avoid excessive object-store metadata operations?
- What metrics would you monitor for object-store bottlenecks?

## Real Use Case

A daily dashboard query has low CPU usage but takes 30 minutes to start. The issue is not executor sizing; Spark is listing hundreds of thousands of small files on object storage. Compaction, table metadata pruning, and better partition layout reduce planning time and storage request cost.
