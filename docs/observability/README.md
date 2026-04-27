# Observability: UI and plans

| Doc | Use it when |
| --- | --- |
| [`spark-ui-guide.md`](spark-ui-guide.md) | You are triaging a run in the **Spark Web UI** or History Server. |
| [`physical-plans.md`](physical-plans.md) | You are reading `EXPLAIN FORMATTED` or the **SQL** tab’s physical plan. |

**Incident workflows:** start with a symptom in [`../troubleshooting/README.md`](../troubleshooting/README.md), then
use the two guides above to gather evidence (metrics + plan) before changing configs.

**Also useful:** event logs and YARN/EMR integration — see
[`../configs/event-logs-and-observability.md`](../configs/event-logs-and-observability.md) and
[`../book/12-production-debugging.md`](../book/12-production-debugging.md).
