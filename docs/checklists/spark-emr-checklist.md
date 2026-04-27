# EMR / YARN + Spark (run checklist)

- [ ] **Driver** placement: **cluster** mode for long jobs; **client** only where understood (notebooks, thin drivers).
- [ ] **Executor** memory + **overhead** fit **YARN** container max; no silent **physical memory** kill.
- [ ] **Spot** — not used for the **only** capacity on **shuffle**-critical SLA jobs; policy documented.
- [ ] **Event log** and **YARN** logs land in a **durable** place for the cluster lifetime.
- [ ] **Release** and **JARs** (Iceberg, connectors) are **pinned** and match **staging** vs **prod**.
- [ ] **IAM** — instance profile / **runtime role** is least privilege for the bucket prefix.
- [ ] **Queues** — the job is in the **intended** **YARN** queue with **fair** / **cap** policy understood.
- [ ] **Autoscaling** — does not starve **streaming** or **latency** jobs (if shared cluster).

**Chapters:** [`../book/11-spark-on-yarn-and-emr.md`](../book/11-spark-on-yarn-and-emr.md) ·
**Troubleshooting:** [`../troubleshooting/emr-yarn-failures.md`](../troubleshooting/emr-yarn-failures.md)
