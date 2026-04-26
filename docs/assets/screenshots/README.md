# Sample outputs (text)

This folder holds **labeled sample text outputs** for the runnable examples in [`examples/`](../../../examples/README.md). They are not production screenshots; regenerate them anytime with the commands in the main [`README.md`](../../../README.md).

| File | Source |
| --- | --- |
| [`explain-formatted-shuffle-output.txt`](explain-formatted-shuffle-output.txt) | Illustrative `EXPLAIN FORMATTED` excerpt (shape matches Spark 3.x; line IDs vary by version). |
| [`skew-detector-output.txt`](skew-detector-output.txt) | Captured from `skew_detector.py` (`--demo` and local CSV). |
| [`file-count-audit-output.txt`](file-count-audit-output.txt) | Captured from `file_count_audit.py --demo`. |

If you add real Spark UI screenshots later, keep them clearly tied to these local runs or to synthetic workloads so they are not mistaken for confidential production captures.
