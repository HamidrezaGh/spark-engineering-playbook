-- ============================================================================
-- Example: Window functions vs GROUP BY (when each is the right shape)
-- ============================================================================
--
-- WHAT THIS DEMONSTRATES
--   Many Spark workloads can be written as either GROUP BY + JOIN, or as a
--   window function. The two shapes have very different physical plans and
--   very different production failure modes.
--
--   We use a common shape: "for each customer, attach their lifetime order
--   total to every order row." The naive way is GROUP BY + JOIN. The
--   window way is a single SUM() OVER (PARTITION BY customer_id).
--
-- WHY IT MATTERS
--   Window functions avoid one shuffle but introduce a sort within the
--   window partition. For wide windows (high cardinality, but each partition
--   is small) this is a huge win. For narrow windows with hot keys, it can
--   be a huge loss because all rows for a hot key must sit in one partition
--   and one task.
--
-- WHAT TO LOOK FOR IN SPARK UI
--   * SQL tab: GROUP BY + JOIN shows two Exchanges (one for the aggregate,
--     one for the join). Window shows one Exchange (partition-by) plus a
--     Sort.
--   * Stages tab: with windows, the stage doing the partition-by + sort is
--     the one to watch. Skewed window partitions show up as long-tail tasks
--     in that stage exactly the same way as GROUP BY skew.
--
-- PHYSICAL PLAN OPERATORS THAT MATTER
--   * Window               -> the window operator, runs after a shuffle+sort.
--   * Exchange hashpartitioning(window_key) -> the partition-by shuffle.
--   * Sort [window_key, order_col]          -> required ordering for the
--                                             window. Often the dominant
--                                             memory consumer.
--
-- PRODUCTION ISSUES THIS HELPS DIAGNOSE
--   * "Window function OOM" -> usually a hot key in PARTITION BY. The whole
--     partition has to fit (or spill) on a single executor.
--   * Excess shuffle from rewriting a window as GROUP BY + JOIN when the
--     window form would have been one shuffle.
--   * Wrong fix: people see "the window is slow" and refactor to GROUP BY +
--     JOIN, which doubles the shuffle. Read the plan first.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Setup assumption: an `orders` table with one row per order.
--   columns: order_id, customer_id, order_date, order_total
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- 1) GROUP BY + JOIN form. Two shuffles.
-- ----------------------------------------------------------------------------

EXPLAIN FORMATTED
WITH lifetime AS (
    SELECT
        customer_id,
        sum(order_total) AS lifetime_total
    FROM orders
    GROUP BY customer_id
)
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_total,
    l.lifetime_total
FROM orders o
JOIN lifetime l
  ON o.customer_id = l.customer_id;

-- Expected plan shape (simplified):
--   Project
--   +- SortMergeJoin [customer_id], [customer_id], Inner
--      :- Exchange hashpartitioning(customer_id, 200)
--      :  +- ... orders scan
--      +- Exchange hashpartitioning(customer_id, 200)
--         +- HashAggregate(keys=[customer_id], functions=[sum(order_total)])
--            +- Exchange hashpartitioning(customer_id, 200)
--               +- HashAggregate(keys=[customer_id], functions=[partial_sum])
--                  +- ... orders scan
-- That's *three* Exchanges in the worst case (one for the partial agg's
-- final, one for each side of the join). AQE often reuses one of them, but
-- you're still doing more shuffle than necessary.

-- ----------------------------------------------------------------------------
-- 2) Window form. One shuffle, one sort.
-- ----------------------------------------------------------------------------

EXPLAIN FORMATTED
SELECT
    order_id,
    customer_id,
    order_date,
    order_total,
    sum(order_total) OVER (PARTITION BY customer_id) AS lifetime_total
FROM orders;

-- Expected plan shape (simplified):
--   Window [sum(order_total) windowspecdefinition(customer_id, ...)],
--          [customer_id], [customer_id ASC NULLS FIRST]
--   +- Sort [customer_id ASC NULLS FIRST], false, 0
--      +- Exchange hashpartitioning(customer_id, 200)
--         +- ... orders scan
--
-- One Exchange. The sort is local within the partition. Wins when each
-- customer has a manageable number of orders.

-- ----------------------------------------------------------------------------
-- 3) When the window form is a TRAP: hot keys.
--    If one customer_id has tens of millions of orders, the entire window
--    partition must be sorted in one task. AQE skew handling for windows
--    is more limited than for joins, so this can OOM where the GROUP BY +
--    JOIN form would only be slow.
--
--    Diagnostic: use 03-skew-detection.sql against orders.customer_id. If
--    the max-to-median ratio is large, prefer the GROUP BY + JOIN form,
--    or pre-aggregate, or salt the window.
-- ----------------------------------------------------------------------------

-- Defensive variant: pre-aggregate, then broadcast-join back. Useful when
-- the customer dimension is small enough to broadcast.

EXPLAIN FORMATTED
WITH lifetime AS (
    SELECT
        customer_id,
        sum(order_total) AS lifetime_total
    FROM orders
    GROUP BY customer_id
)
SELECT /*+ BROADCAST(l) */
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_total,
    l.lifetime_total
FROM orders o
JOIN lifetime l
  ON o.customer_id = l.customer_id;

-- ----------------------------------------------------------------------------
-- Decision rule (production):
--   * Window if PARTITION BY cardinality is high and per-partition row
--     count is moderate.
--   * GROUP BY + BROADCAST JOIN if the aggregate output is small enough
--     to broadcast (a few hundred MB safely).
--   * GROUP BY + SORT-MERGE JOIN only when neither of the above fits.
-- ----------------------------------------------------------------------------
