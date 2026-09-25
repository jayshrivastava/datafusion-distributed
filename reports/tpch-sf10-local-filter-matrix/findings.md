## Findings

These runs do not show a general speedup from enabling dynamic filtering.
Q18 with all three options enabled is the only mean-latency improvement.

- **Dynamic filtering alone:** every enabled case receives remote updates,
  but none prunes any row groups. Scan output rows are identical to all-off,
  and bytes scanned are essentially identical. Mean latency is 1-8% worse.
- **Q17:** all-on is 1.83x slower despite removing 62.4 million rows at scans.
  Scan predicate evaluation totals 18.9 seconds across tasks/partitions, not
  wall-clock time. The runtime predicates contain partition-routing CASEs and
  large IN lists. Reduced join/shuffle work does not offset scan-filter cost.
- **Q18:** all-on is 1.11x faster, from 2023.8 to 1824.4 ms. The orders scan
  shrinks from 15 million to 2.89 million rows, and the second lineitem scan
  from 59.99 million to 33.87 million rows on average. The initial lineitem
  aggregation scan remains unfiltered. This is row filtering without dynamic
  row-group pruning, and the reduced join inputs are visible in the metrics.
- **Q20:** all-on is 1.43x slower. Most scan-row savings are associated with
  the existing date/name predicates being evaluated inside Parquet. The
  partsupp scan only shrinks from 8 million to 7.997 million rows, so its
  remote join filters provide little selectivity in this run.
- **Q15:** all-on is 1.41x slower. Its remote scan predicate is
  `l_suppkey >= 1 AND l_suppkey <= 100000 AND true`, covering the entire
  supplier domain. Both lineitem scans emit 2,265,714 rows with pushdown,
  reflecting the static three-month date restriction, not a selective remote
  supplier filter. The local revenue filter reduces the outer join input
  from 100,000 rows to one, but only after the revenue aggregations.

All numbers above use the ten measured iterations, excluding warm-ups.
Result rows match the all-off baseline in every iteration: q17=1, q18=624,
q20=1804, and q15=1. Full result values are retained in `samples.json`.

This matrix does not isolate the effect of reordering from Parquet pushdown,
or remote filtering from local filtering. It therefore cannot attribute all
of the all-on change to one of those mechanisms.
