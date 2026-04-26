# S3 With Spark On EMR


## Core Idea

S3 is not HDFS. It is durable, scalable object storage with different semantics and performance characteristics than a distributed filesystem. Spark on EMR must account for S3 list cost, request rates, object metadata, commit behavior, small files, S3 throttling, and non-atomic rename-like workflows.

## Key Takeaways

- **S3 is not a POSIX filesystem**, so rename/list/write patterns matter.
- **Small files create S3 request cost and Spark planning overhead**.
- **Low executor CPU can mean S3 is the bottleneck**.
- **Adding executors can make S3 pressure worse** if request rate or listing is the limit.

## Mental Model

HDFS is built for distributed filesystem operations. S3 exposes object APIs. Operations such as listing many prefixes, writing many small objects, and renaming paths can be expensive or implemented as copy/delete workflows.

```text
Filesystem intuition:
rename(pathA, pathB) -> cheap metadata operation

S3 reality:
rename-like behavior -> copy object(s) + delete old object(s)
listing many prefixes -> many remote metadata requests
many tiny files -> many remote reads and request charges
```

| Operation | HDFS Intuition | S3 Reality |
| --- | --- | --- |
| Rename | Cheap and atomic | Often copy/delete and expensive |
| List directory | Metadata lookup | Remote API calls over many objects |
| Write many files | Mostly local cluster concern | Request cost, commit cost, planning cost |
| Read partitioned table | Directory pruning | Depends on metadata, listings, and layout |

## What Spark Does Internally

Spark on EMR reads S3 through Hadoop filesystem integrations such as S3A and EMRFS depending on EMR version and configuration. Spark scans S3 by listing paths and planning file reads. Many small files mean many S3 metadata operations and many tiny tasks. During writes, commit protocols must make task outputs visible safely even though S3 rename-like behavior may not be atomic or cheap.

Table formats reduce risk by committing metadata snapshots rather than relying only on directory state.

On EMR, the practical read/write path often looks like:

```text
Spark task
  -> Hadoop S3 client: S3A or EMRFS
  -> S3 API calls
      |-- ListObjects / metadata requests
      |-- GetObject reads
      |-- PutObject writes
      |-- copy/delete for rename-like workflows
```

## Why It Matters In Production

S3 bottlenecks often look like Spark slowness:

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
- KMS throttling or access errors for encrypted buckets.
- IAM policy issues that only appear on executors.
- High S3 request volume from small-file reads or writes.

## Tuning And Configuration

Tune S3-heavy EMR workloads by:

- Reducing small files.
- Avoiding unnecessary listings.
- Using table metadata for pruning.
- Choosing appropriate S3 committers or table-format commit paths.
- Controlling write parallelism.
- Compacting data files.
- Designing partition layouts that do not create too many prefixes or files.
- Watching S3 request rate, retry, and throttling signals.
- Avoiding the false fix of adding more executors when the bottleneck is S3 metadata/listing.
- Using S3-backed Spark event logs so slow S3 behavior can be diagnosed after cluster termination.

## Operating Signals

Monitor:

- S3 request count and throttling.
- File count scanned.
- Listing time.
- Read/write throughput.
- Commit time.
- Average file size.
- Retry counts from storage clients.
- CloudWatch S3 metrics where available.
- KMS throttling or access-denied errors for encrypted data.

## Best Practices

- Use transactional table formats for important datasets.
- Keep files reasonably sized.
- Use partition pruning and metadata pruning.
- Avoid treating S3 as POSIX storage.
- Separate storage bottlenecks from Spark compute bottlenecks during debugging.
- Prefer EMR/Spark configurations and table formats that avoid rename-heavy commit behavior.
- Keep S3 bucket layout, Glue catalog metadata, and table-format metadata aligned.

## Anti-Patterns

- Writing thousands of tiny objects per batch.
- Depending on directory listing as the source of truth for table state.
- Using high-cardinality path partitioning.
- Assuming rename is atomic and cheap everywhere.
- Debugging S3-heavy jobs only from executor CPU metrics.
- Granting broad S3 permissions instead of explicit prefixes and KMS keys.

## Example

```python
spark.read.parquet("s3://lake/events/").where("event_date = '2026-04-25'")
```

This is efficient only if the table layout and metadata let Spark avoid listing and scanning irrelevant S3 objects.

## Interview-Style Questions Covered

- Why is S3 not the same as HDFS?
- Why are rename operations expensive or unsafe on object stores?
- How do list operations affect Spark planning and runtime?
- How do small files affect S3 cost and latency?
- What are common symptoms of S3 throttling?
- How do you tune Spark for S3-heavy workloads on EMR?
- How do EMRFS and S3A committers reduce S3 write problems?
- How do table formats like Iceberg, Delta, and Hudi reduce object-store risks?
- How do you design a pipeline to avoid excessive object-store metadata operations?
- What CloudWatch and Spark metrics would you monitor for S3 bottlenecks?

## Real Use Case

A daily dashboard query on EMR has low executor CPU usage but takes 30 minutes to start. The issue is not executor sizing; Spark is listing hundreds of thousands of small files on S3. Compaction, Iceberg/Glue metadata pruning, and better partition layout reduce planning time, S3 request cost, and false pressure to scale the cluster.
