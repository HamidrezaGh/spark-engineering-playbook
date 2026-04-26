# Tuning

Tuning guides focus on one performance lever at a time. They should explain the knob, when it helps, when it hurts, how to validate the change, and what production signals to monitor.

## Index

| Guide | Focus |
| --- | --- |
| [Executor Sizing](executor-sizing.md) | Cores, heap, overhead, and concurrent tasks per executor on EMR-style clusters. |
| [Shuffle Partitions](shuffle-partitions.md) | `spark.sql.shuffle.partitions`, AQE coalesce, and output file shape. |
| [Broadcast Joins](broadcast-joins.md) | When broadcast wins, when it OOMs, and how to validate join strategy in the UI. |
| [Memory Overhead](memory-overhead.md) | JVM vs container limits, PySpark, and YARN kill signatures. |
| [S3 On EMR](object-storage.md) | Listing, throttling, and commit behavior that show up as “slow Spark” on object storage. |
