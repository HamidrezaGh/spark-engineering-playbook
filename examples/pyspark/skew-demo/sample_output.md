# Sample output (illustrative)

`skew_demo.py` ends with a block similar to the shared skew detector, for example:

```text
=== Skew report for key: customer_id ===
  total rows           : 49
  distinct keys        : ...
  max/median ratio     : 12.0
  classification       : moderate-skew
  top keys:
    cust_001 ...
```

(Exact numbers depend on the sample CSV; the **demo** path unions extra rows to force a hot
`cust_001`.)

`salted_join_fix.py` prints a formatted plan containing `BroadcastHashJoin` and a small row
count (join_key alignment must match the replicated right side for correctness).
