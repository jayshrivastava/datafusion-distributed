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
