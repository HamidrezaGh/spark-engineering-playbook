# Spark Configs

This section is a practical field manual for Spark configuration in production (with an EMR/YARN/S3 bias).

The goal is not to list every config. The goal is to cover the configs that most often control cost, performance, and failure modes, and to explain **how to validate changes in Spark UI**.

## How To Use This Section

- Start from the symptom and Spark UI evidence in `docs/field-guides/spark-ui-reading-guide.md`.
- Use these pages to choose the smallest config change that matches the bottleneck.
- Validate with “before vs after” Spark UI signals (stages, SQL plan, executors).

## Index

- [Principles](principles.md)
- [Top Spark Configs (Cheat Sheet)](top-configs.md)
- [Execution And Parallelism](execution-and-parallelism.md)
- [Shuffle](shuffle.md)
- [Joins And Broadcast](joins-and-broadcast.md)
- [Adaptive Query Execution (AQE)](aqe.md)
- [Memory And GC](memory-and-gc.md)
- [S3 / Object Storage IO](object-storage-io.md)
- [Dynamic Allocation And Autoscaling](dynamic-allocation.md)
- [Speculation And Stragglers](speculation.md)
- [Event Logs And Observability](event-logs-and-observability.md)
