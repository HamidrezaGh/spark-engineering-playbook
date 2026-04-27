# Spark UI reading guide (field guide)

The **canonical**, tab-by-tab **Spark Web UI** reference lives in
[`../observability/spark-ui-guide.md`](../observability/spark-ui-guide.md). Use that file for
**Jobs, Stages, Tasks, SQL, Executors, Storage,** and **Environment**, plus the signal →
interpretation → fix tables.

**Triage in one line:** open **Stages** (duration + task spread), then **SQL** (plan vs slow stage), then
**Executors** (health). Keep this loop the same for every incident so speed comes from habit, not
re-invention.

**Related field guides:** [`debugging-slow-jobs.md`](debugging-slow-jobs.md),
[`debugging-skew.md`](debugging-skew.md), [`debugging-oom.md`](debugging-oom.md), and
[`../troubleshooting/slow-job.md`](../troubleshooting/slow-job.md).

**Plan reading:** [`../observability/physical-plans.md`](../observability/physical-plans.md).
