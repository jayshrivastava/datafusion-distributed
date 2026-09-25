# TPC-H SF10: Post-Scan Dynamic Filters

Uncommitted experiment on `js/6-apply-merged-filters`.

## Setup

- Release build; four localhost worker processes.
- Four worker threads and four target partitions per worker.
- One warm-up and ten measured executions per matrix entry.
- Static task planning; existing SF10 Parquet data; warm filesystem cache.
- Metrics and dynamic-filter reporting enabled in every case.
- All 132 results match the previous all-off results exactly.
- Planning and plan rendering are outside the execution timer.

| Case | Dynamic filters | Parquet pushdown | Reordering |
| --- | --- | --- | --- |
| all_off | false | false | false |
| dynamic_only | true | false | false |
| all_on | true | true | true |

Ordinary Parquet statistics pruning stays enabled in all cases.
The post-scan fallback is active only in `dynamic_only`.

## Latencies

Milliseconds over ten runs. Speedup is the new all-off mean divided by
the case mean. A value below 1 means slower.

| Query | Case | Mean | Median | Min | Max | Speedup |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| q17 | all_off | 1845.7 | 1791.9 | 1700.0 | 2031.6 | 1.00x |
| q17 | dynamic_only | 3391.2 | 3393.4 | 3313.5 | 3491.4 | 0.54x |
| q17 | all_on | 3421.4 | 3429.9 | 3276.2 | 3638.0 | 0.54x |
| q18 | all_off | 2146.0 | 2095.0 | 1990.2 | 2457.5 | 1.00x |
| q18 | dynamic_only | 1908.0 | 1933.5 | 1583.4 | 2138.1 | 1.12x |
| q18 | all_on | 1930.0 | 1914.1 | 1753.7 | 2130.0 | 1.11x |
| q20 | all_off | 535.0 | 518.5 | 483.1 | 622.3 | 1.00x |
| q20 | dynamic_only | 1039.9 | 1035.9 | 931.0 | 1192.7 | 0.51x |
| q20 | all_on | 803.6 | 835.0 | 697.2 | 886.3 | 0.67x |
| q15 | all_off | 452.4 | 437.0 | 406.9 | 529.9 | 1.00x |
| q15 | dynamic_only | 478.7 | 462.9 | 433.2 | 540.6 | 0.95x |
| q15 | all_on | 630.7 | 632.3 | 588.5 | 667.2 | 0.72x |

## Compared With the Previous Run

Historical means, not an interleaved before/after experiment. The all-off
and all-on execution paths were not intentionally changed; their movement
provides context for host and scheduling variability.

| Query | Case | Previous ms | New ms | Previous / new |
| --- | --- | ---: | ---: | ---: |
| q17 | all_off | 1902.4 | 1845.7 | 1.03x |
| q17 | dynamic_only | 2045.4 | 3391.2 | 0.60x |
| q17 | all_on | 3486.5 | 3421.4 | 1.02x |
| q18 | all_off | 2023.8 | 2146.0 | 0.94x |
| q18 | dynamic_only | 2052.8 | 1908.0 | 1.08x |
| q18 | all_on | 1824.4 | 1930.0 | 0.95x |
| q20 | all_off | 528.9 | 535.0 | 0.99x |
| q20 | dynamic_only | 554.6 | 1039.9 | 0.53x |
| q20 | all_on | 757.4 | 803.6 | 0.94x |
| q15 | all_off | 431.7 | 452.4 | 0.95x |
| q15 | dynamic_only | 458.0 | 478.7 | 0.96x |
| q15 | all_on | 610.5 | 630.7 | 0.97x |

## Rows After Scans

Mean row counts in millions, summed across scans, including repeated table
scans. Forwarded rows subtract the new immediate post-scan filter drops.
Other downstream filters are not subtracted.

| Query | Case | Scan output M | Post-scan removed M | Forwarded M |
| --- | --- | ---: | ---: | ---: |
| q17 | all_off | 121.972 | 0.000 | 121.972 |
| q17 | dynamic_only | 121.972 | 74.883 | 47.089 |
| q17 | all_on | 57.738 | 0.000 | 57.738 |
| q18 | all_off | 136.472 | 0.000 | 136.472 |
| q18 | dynamic_only | 136.472 | 57.538 | 78.934 |
| q18 | all_on | 73.518 | 0.000 | 73.518 |
| q20 | all_off | 70.086 | 0.000 | 70.086 |
| q20 | dynamic_only | 70.086 | 0.125 | 69.961 |
| q20 | all_on | 17.112 | 0.000 | 17.112 |
| q15 | all_off | 120.072 | 0.000 | 120.072 |
| q15 | dynamic_only | 120.072 | 0.000 | 120.072 |
| q15 | all_on | 4.631 | 0.000 | 4.631 |

## Filter Costs and Pruning

Compute milliseconds sum across tasks, not wall-clock query latency.
Parquet pushdown counts include both static and dynamic predicates.

| Query | Case | Post-scan compute ms | Pushdown eval ms | Dynamic RG pruned |
| --- | --- | ---: | ---: | ---: |
| q17 | all_off | 0.0 | 0.0 | 0.0 |
| q17 | dynamic_only | 23292.9 | 0.0 | 0.0 |
| q17 | all_on | 0.0 | 19157.7 | 0.0 |
| q18 | all_off | 0.0 | 0.0 | 0.0 |
| q18 | dynamic_only | 10821.9 | 0.0 | 0.0 |
| q18 | all_on | 0.0 | 11425.5 | 0.0 |
| q20 | all_off | 0.0 | 0.0 | 0.0 |
| q20 | dynamic_only | 4410.2 | 0.0 | 0.0 |
| q20 | all_on | 0.0 | 1099.8 | 0.0 |
| q15 | all_off | 0.0 | 0.0 | 0.0 |
| q15 | dynamic_only | 115.2 | 0.0 | 0.0 |
| q15 | all_on | 0.0 | 114.8 | 0.0 |

## Shuffle Traffic

MiB transferred by NetworkShuffleExec, averaged over ten runs.

| Query | All off | Dynamic only | All on |
| --- | ---: | ---: | ---: |
| q17 | 898.1 | 325.8 | 427.3 |
| q18 | 798.1 | 161.0 | 134.0 |
| q20 | 137.2 | 137.1 | 137.2 |
| q15 | 17.5 | 17.5 | 17.5 |

## Findings

Post-scan filtering works, but this implementation should remain experimental.
It makes q18 modestly faster, substantially regresses q17 and q20, and does
no useful scan filtering for q15. No case pruned dynamic row groups; scanned
bytes were effectively unchanged. Reading fewer rows downstream is not the
same as reading fewer bytes from Parquet.

### Q17

- Post-scan filters removed 74.88 million rows on average. Total scan output
  remained 121.97 million; only 47.09 million rows left the scans plus their
  immediate filters. Shuffle traffic fell from 898.1 to 325.8 MiB.
- Nevertheless, dynamic-only took 3,391 ms versus 1,846 ms all-off, or 1.84x
  slower. The added filters spent 23.29 seconds of summed compute across
  tasks. Their predicates contain ORed partition CASE expressions and large
  IN sets; evaluation and batch filtering outweighed the downstream savings.
- The previous pruning-only dynamic case averaged 2,045 ms. The new fallback
  is clearly more expensive here; fewer forwarded rows alone is not a win.

### Q18

- Dynamic-only averaged 1,908 ms versus 2,146 ms all-off: 1.12x faster.
  All-on averaged 1,930 ms, close to dynamic-only in this run.
- Filters on orders and the second lineitem scan removed 57.54 million rows
  on average. Shuffle traffic dropped from 798.1 to 161.0 MiB, about 80%.
  The first lineitem aggregation scan remains necessary.
- Update timing matters: combined output from the two post-scan filters
  ranged from 4,992 to 61.29 million rows across measured runs. The final
  displayed predicate does not tell you when it became available. This is
  a modest local improvement, not evidence of a large general speedup.

### Q20

- Dynamic-only averaged 1,040 ms versus 535 ms all-off: 1.94x slower.
  The post-scan filters consumed 4.41 seconds of summed compute.
- Only 124,736 of 68.09 million eligible rows were removed, about 0.18%.
  The supplier nation filter is selective, but the expensive partsupp and
  lineitem predicates mostly contain broad key bounds. Shuffle traffic was
  essentially unchanged at about 137 MiB.
- All-on also remained slower than all-off. Its much smaller scan output
  includes the static name/date filters, not just remote dynamic filtering.

### Q15

- The new scan filter is effectively `1 <= l_suppkey <= 100000`. That covers
  every supplier key in this SF10 dataset: it processed 59,986,052 rows and
  removed zero in every measured run.
- Dynamic-only averaged 479 ms versus 452 ms all-off. Its movement relative
  to the previous run roughly tracks the all-off control; there is no
  demonstrated performance benefit from adding this scan filter.
- The existing intermediate revenue filter still reduces 100,000 rows to
  one. That happens after aggregation and the supplier/revenue join; it
  does not save the large lineitem scan or aggregation work.

## Display Fix

Stage 6's intermediate filter now shows each task's actual predicate:

```text
total_revenue >= 2194132.8166
AND total_revenue <= 2194132.8166
AND total_revenue IN (2194132.8166)
```

Previously, the executed plan displayed `DynamicFilter [ empty ]` there,
despite the metrics proving it filtered rows. Completed reports were replayed
only into distributed leaf variants. They now also populate isolated,
per-task copies of intermediate FilterExec nodes for display.

The display wrapper keeps the original tree shape, so collected metrics still
refer to the same operator positions. It never mutates the original plan.
All dynamic-enabled q15 plans in this run show the intermediate predicate.

## Scope and Caveats

- The fallback adds only dynamic predicates, not duplicate static filters.
- It is disabled when Parquet row pushdown is enabled. Dynamic expression
  references are remapped to projected scan columns using DataFusion's API.
- Embedded scan limits and unavailable projected columns are skipped.
- All configurations used the same task counts per query: 15 tasks for q17,
  24 for q18, 24 for q20, and 18 for q15, excluding the coordinator head.
- The comparison with the previous binary is historical, not interleaved.
  Within this run, configuration order rotates and reverses between rounds.
- Other planner snapshot suites have not been comprehensively rebaselined
  for this experimental new operator. The dynamic-filter suites and the
  selected SF10 benchmark results are the validation performed here.

Before enabling this broadly, predicate evaluation cost and weak filters
need attention. In particular, q20 pays for broad partitioned predicates
over almost every decoded row. Q17 also warrants profiling its CASE/IN
evaluation; the benchmark establishes the expensive operator, not a
function-level CPU attribution.

## Executed Plans

Representative plans are from a median-near measured execution. Every
warm-up and measured plan is also retained in `plans/`.

| Query | All off | Dynamic only | All on |
| --- | --- | --- | --- |
| q17 | [plan](plans/q17-all_off.txt) | [plan](plans/q17-dynamic_only.txt) | [plan](plans/q17-all_on.txt) |
| q18 | [plan](plans/q18-all_off.txt) | [plan](plans/q18-dynamic_only.txt) | [plan](plans/q18-all_on.txt) |
| q20 | [plan](plans/q20-all_off.txt) | [plan](plans/q20-dynamic_only.txt) | [plan](plans/q20-all_on.txt) |
| q15 | [plan](plans/q15-all_off.txt) | [plan](plans/q15-dynamic_only.txt) | [plan](plans/q15-all_on.txt) |

## Artifacts

- [Raw timings, result rows, and operator metrics](samples.json)
- [Machine-readable summary](summary.json)
- [Implementation scope](README.md)
- [Previous report](../tpch-sf10-local-filter-matrix/report.md)
- [Runner log](run.log)
- [Metadata](metadata.json)

These four queries do not establish a whole-suite performance improvement.
The all-on case also changes static row filtering, so its benefit cannot
be attributed solely to remote dynamic filters.
