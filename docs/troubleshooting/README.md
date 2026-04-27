# Production troubleshooting

Decision trees for common Spark production symptoms. Use them after the 90-second triage in
[`../observability/spark-ui-guide.md`](../observability/spark-ui-guide.md) when you need a
structured branch-by-branch path.

| Symptom / topic | Guide |
| --- | --- |
| General slowness | [slow-job.md](slow-job.md) |
| Long-tail tasks, hot keys | [skew-and-stragglers.md](skew-and-stragglers.md) |
| OOM, spill, GC | [memory-spill-oom.md](memory-spill-oom.md) |
| Huge shuffle read/write | [shuffle-heavy-job.md](shuffle-heavy-job.md) |
| Joins slower than expected | [join-performance.md](join-performance.md) |
| Many tiny output files | [small-files.md](small-files.md) |
| YARN, EMR, container loss | [emr-yarn-failures.md](emr-yarn-failures.md) |
| Iceberg `MERGE`, commits | [iceberg-merge-issues.md](iceberg-merge-issues.md) |
| Streaming lag, checkpoints | [streaming-lag.md](streaming-lag.md) |

For narrative field guides, see [`../field-guides/README.md`](../field-guides/README.md).
