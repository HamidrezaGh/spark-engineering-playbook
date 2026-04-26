# Speculation And Stragglers


## Core Idea

Speculation reruns slow tasks to reduce tail latency. It can help when stragglers are caused by flaky nodes, but it can also amplify load during shuffle-heavy stages.

## When It Helps

- Intermittent slow tasks due to noisy nodes or transient issues.
- Long-tail stages where the slow tasks are not caused by skewed input size.

## When It Hurts

- If the “slow task” is slow because it has more data (skew), speculation duplicates expensive work and increases cluster pressure.
- During shuffle-heavy stages, speculation can increase network and disk contention.

## UI-First Validation

- Stages: compare slow-task input/shuffle sizes to median.
  - If slow tasks have much larger input/shuffle read, it’s likely skew (speculation won’t fix root cause).
  - If slow tasks have similar sizes but one is slow, speculation may help.
