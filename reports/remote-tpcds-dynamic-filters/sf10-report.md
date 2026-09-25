# Remote TPC-DS SF10 Results

**Complete, 2026-09-25:** 600 timed executions, 120 excluded warmups,
60 complete cases, and a plan with metrics for every execution. No query
failed, result row counts stayed consistent, and worker checks passed.
**SF100 is paused for review.** Its earlier 200 measurements remain
provisional; the remaining 400 have not run.

## Findings

All ten queries completed all six configurations, with ten measured runs
per case. Five reproduce a repeated, metric-supported gain in at least
one remote configuration. The table includes all ten, showing one
representative configuration each; the full six-case matrix follows.
These compare dynamic filtering with its disabled control, not A with B.

| Query | Case | Off ms | On ms | Gain | Result | Remote producers | Local producers |
| --- | --- | ---: | ---: | ---: | --- | --- | --- |
| Q21 | A-off | 930.1 | 280.4 | 3.32x | Repeated win | None | CollectLeft joins |
| Q37 | A-on | 1,720.7 | 717.5 | 2.40x | Repeated win | Partitioned join, TopK | CollectLeft joins |
| Q39 | A-off | 1,840.3 | 777.0 | 2.37x | Repeated win | None | CollectLeft joins |
| Q82 | A-off | 1,060.7 | 521.3 | 2.03x | Repeated win | Partitioned join, TopK | CollectLeft joins |
| Q26 | A-off | 1,054.4 | 823.1 | 1.28x | Repeated win | CollectLeft/partitioned joins, TopK | None |
| Q25 | A-off | 1,586.5 | 1,449.2 | 1.09x | Inconclusive | CollectLeft/partitioned joins | None |
| Q98 | B-off | 739.1 | 757.9 | 0.98x | Inconclusive | None | CollectLeft joins |
| Q27 | A-off | 1,677.5 | 1,728.0 | 0.97x | Inconclusive | CollectLeft/partitioned joins, TopK | None |
| Q80 | A-off | 1,300.2 | 1,362.5 | 0.95x | Inconclusive | CollectLeft joins | None |
| Q17 | A-off | 1,095.8 | 1,256.4 | 0.87x | Slower | CollectLeft/partitioned joins | None |

A gain below 1.0x means a slower mean. Inconclusive cases have an interval
that crosses 1.0x and do not establish a repeated improvement or slowdown.

The off/on milliseconds above mean dynamic filtering disabled/enabled
with the same Parquet settings. Producer columns describe relationships,
not independent attribution to every listed producer. No query here has
an ungrouped MIN/MAX producer.

- **Q26 is the clearest remote-only example:** A-off wins 1.28x; B-off
  wins 1.32x. It prunes pages, reads fewer bytes, and sends less data.
  Both beat the fastest disabled-filter configuration.
- **The larger gains are local-only or mixed.** Q21 and Q39 use local
  filters at SF10. Q37 and Q82 also prune inventory locally; their
  remote sales-side pruning is visible when row pushdown is enabled.
- **Q80's large local win does not transfer.** Its remote filters work,
  but lower row counts and shuffle volume do not improve latency.
  Q27, Q17, and Q98 have no repeated gain either. Q25's best mean gain
  is only 1.09x and changes direction between passes.

Q37 A-off is faster in absolute time than the A-on example above:
573.3 ms, or 1.65x against Control-off. A-on's 2.40x becomes only 1.32x
against the faster disabled configuration. Similarly, Q26 B-on's 1.51x
matched gain loses to Control-off; A-off/B-off are better choices.

## Scope

The ten queries are the top ten TPC-DS candidates from the
[local investigation](../local-dynamic-filter-sweep/findings.md): Q21,
Q37, Q80, Q39, Q27, Q25, Q82, Q17, Q26, and Q98.

Each query has six cases, with ten measured executions per case across
two fresh-worker passes. Each five-run block has an excluded warmup.
SF10 completed 600 timed executions and 120 warmups. Every execution,
including each warmup, has a saved plan with metrics.

| Case | Dynamic filtering | Parquet pushdown/reordering |
| --- | --- | --- |
| Control-off | Disabled | Both off |
| Control-on | Disabled | Both on |
| A-off | Existing partitioned-join OR merge | Both off |
| A-on | Existing partitioned-join OR merge | Both on |
| B-off | Global-hash CASE experiment | Both off |
| B-on | Global-hash CASE experiment | Both on |

Normal Parquet statistics and page-index pruning remain enabled in all
six cases. Neither A nor B injects post-scan FilterExecs. Both keep the
same incremental TopK/aggregate merge behavior. Settings are applied
before recreating the Parquet table providers and verified for every block.

See the [matrix](matrix.md) for every case, its mean, median, spread,
individual pass means, and representative executed plan. The
[numeric summary](summary.json) retains every timed sample and parsed
plan counter. Raw benchmark JSON and all executed plans are under `raw/`.

## Full SF10 Matrix

Arithmetic mean milliseconds, ten samples in every cell:

| Query | Control-off | Control-on | A-off | A-on | B-off | B-on |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q21 | 930.1 | 887.5 | 280.4 | 411.0 | 458.8 | 545.1 |
| Q37 | 945.1 | 1,720.7 | 573.3 | 717.5 | 589.0 | 928.6 |
| Q80 | 1,300.2 | 1,307.4 | 1,362.5 | 1,377.4 | 1,608.8 | 1,561.4 |
| Q39 | 1,840.3 | 1,895.8 | 777.0 | 1,245.0 | 837.7 | 1,294.5 |
| Q27 | 1,677.5 | 2,179.6 | 1,728.0 | 2,393.2 | 2,017.1 | 2,401.9 |
| Q25 | 1,586.5 | 1,520.1 | 1,449.2 | 1,722.1 | 1,897.4 | 2,161.4 |
| Q82 | 1,060.7 | 1,675.3 | 521.3 | 867.2 | 685.1 | 1,106.4 |
| Q17 | 1,095.8 | 1,157.6 | 1,256.4 | 1,486.8 | 1,281.1 | 1,623.3 |
| Q26 | 1,054.4 | 1,931.6 | 823.1 | 1,855.9 | 801.0 | 1,278.5 |
| Q98 | 739.1 | 803.0 | 824.0 | 1,476.7 | 757.9 | 978.0 |
| Sum | 12,229.7 | 15,078.7 | 9,595.0 | 13,552.9 | 10,934.0 | 13,879.1 |

For these selected queries, A-off is 1.27x and B-off 1.12x faster in
summed mean latency than Control-off. The on cases are 1.11x and 1.09x
faster than Control-on, but both lose to Control-off. These sums are not
a whole-TPC-DS-suite result and do not have an aggregate confidence bound.

The [linked matrix](matrix.md#tpcdssf10) makes every case a link to its
representative plan. [Detailed findings](findings.md) include confidence
intervals and metric evidence for the selected cases.

## Same Local Case, Remote Result

This keeps the locally selected configuration fixed. A value below 1.0x
means dynamic filtering lost against its matching disabled control.

| Query | Local selected case | Local gain | Remote gain |
| --- | --- | ---: | ---: |
| Q21 | B-off | 4.34x | 2.03x |
| Q37 | A-on | 3.78x | 2.40x |
| Q80 | B-on | 2.82x | 0.84x |
| Q39 | B-off | 2.61x | 2.20x |
| Q27 | A-on | 2.41x | 0.91x |
| Q25 | A-on | 2.37x | 0.88x |
| Q82 | A-on | 2.26x | 1.93x |
| Q17 | B-on | 2.05x | 0.71x |
| Q26 | B-on | 2.02x | 1.51x |
| Q98 | B-on | 1.97x | 0.82x |

Q21 and Q39's remote A-off cases are faster than their B-off cases, but
neither query uses a global-hash merge at SF10. Do not attribute those
A/B latency differences to different filter selectivity.

## What Changed From Local

The SF10 input is the same: all 384 local Parquet files match the S3
object checksums, including multipart ETags. That is 12,932,037,028 bytes
across 24 tables. See [the verification](data-comparison.json).
SQL and the frozen A/B library sources also match the local experiment.

| Aspect | Local confirmation | Remote SF10 |
| --- | --- | --- |
| Hardware | One 16-core ARM host | Twelve c5n.4xlarge nodes |
| Workers | Four localhost gRPC processes | Twelve pods, one per node |
| CPU/memory | Shared host, about 61.4 GiB | 15 CPU and 38 GiB per pod |
| Target partitions | 4 per worker | 15 per worker |
| Task cap per stage | 4 | Uncapped default, 0 |
| Coordinator | Separate process, two threads | One of the worker pods |
| Input storage | Warm local instance storage | S3 |
| Timer | Execute and drain results | Also physical planning and reporting |
| Samples per case | 20, paired across two sessions | 10 across two blocks |
| B-disabled control | Same B binary | Bracketing A control passes |

The remote workers are the existing dedicated nodes, not the tool's
current c5n.2xlarge / 7 CPU / 17 GiB defaults. No worker shape changed
during measurement. Static planning, broadcast joins, union isolation,
LZ4 compression, metrics, and normal Parquet pruning are enabled in both
environments. No other benchmark shares the measured nodes.

Raw local and remote milliseconds are not an apples-to-apples hardware
comparison. Compare dynamic filtering against its disabled control inside
each environment first. The [same-case comparison](local-vs-remote.md)
holds the locally selected A/B and Parquet mode fixed, instead of comparing
different winning configurations.

These changes are not individually isolated experiments. More tasks,
cross-node traffic, S3 access, different CPUs, and the wider timer are
plausible contributors to changed latency. The plans establish changed
work; they do not measure how much wall time each of those factors adds.

## Why The Larger Wins Hold

- **Q21:** inventory output falls from 133.12 M to 4.80 M rows and
  scanned bytes from 458.20 to 21.20 MB. Statistics prune 112 row groups
  and page indexes prune another 11.98 M rows. Remote updates stay zero:
  this is genuine local-filter pruning. See [disabled][q21-control] and
  [enabled][q21-a].
- **Q39:** across its two inventory branches, output falls from 266.24 M
  to 4.62 M rows, and read volume from 916.40 to 29.26 MB. It also has
  zero remote updates at SF10. See [disabled][q39-control] and
  [enabled][q39-a]. Its SF100 route changes, so the SF10 classification
  must not be carried over without inspecting the SF100 plan.
- **Q37/Q82:** local inventory pruning removes most scanned bytes in
  the fast A-off cases. With A-on, remote join filters additionally
  reduce catalog-sales output from 14.40 M to 536 rows in Q37 and
  store-sales output from 28.80 M to 2,505 rows in Q82. Those scans
  do not reduce output with A-off. The on plans prove remote row
  filtering works, but their entire speedup cannot be assigned to it.
  See [Q37 disabled][q37-control], [Q37 enabled][q37-a],
  [Q82 disabled][q82-control], and [Q82 enabled][q82-a].

## Q26: A Remote-Filter Win

With Parquet pushdown/reordering off, A improves from **1,054.4 to
823.1 ms, or 1.28x**. B reaches **801.0 ms, or 1.32x**. A's two enabled
block means are 791.4 and 854.7 ms, below both control block means,
1,081.5 and 1,027.3 ms. Its within-block bootstrap interval is 1.15-1.43x.

The plan has remote CollectLeft and partitioned join producers in stage 6
feeding catalog-sales consumers in stage 5. No local consumer is needed
for this example. A TopK relationship also exists, but its presence alone
is not evidence that it contributes to the speedup.

| A-off metric | Dynamic filtering off | Dynamic filtering on |
| --- | ---: | ---: |
| Catalog-sales scan rows | 14.40 M | 3.20 M |
| Catalog-sales scanned bytes | 789.85 MB | 196.73 MB |
| Page-index rows pruned | 0 | 11.20 M |
| Row-pushdown rejects | 0 | 0 |
| Join input rows, summed | 15.96 M | 4.59 M |
| Network bytes, summed | 294.17 MB | 85.55 MB |
| Remote filter updates | 0 | 103.3 |
| Tasks | 40 | 40 |

Here, MB means 1,000,000 bytes. Counters are means of rounded plan metrics.
See the [disabled plan][q26-control], [A-off plan][q26-a], and
[B-off plan][q26-b]. This is useful **page pruning without Parquet row
pushdown**, not a row-group-pruning or injected-FilterExec result.

The A/B off cases prune essentially the same amount, so the small A/B
latency difference is not evidence for additional selectivity from B.
This experiment also does not isolate the partitioned join's benefit
from the other remote join predicates.

B-on improves 1.51x against Control-on, but its 1,278.5 ms is slower than
Control-off's 1,054.4 ms. The local B-on result was 2.02x. Use A-off or
B-off for the remote example, not B-on's larger matched-control ratio.

## Q80: Pruning Survives, The Speedup Does Not

Local A-on improved from 1,742.0 to 640.1 ms, **2.72x**. The same remote
case goes from 1,307.4 to 1,377.4 ms, **0.95x**. Neither remote A-on
block beats its same-pass control. B-on also fails to reproduce its
local 2.82x win: the remote ratio is 0.84x.

| A-on metric | Remote disabled | Remote enabled | Local enabled |
| --- | ---: | ---: | ---: |
| Scan output rows | 55.47 M | 6.14 M | 6.23 M |
| Scanned bytes | 2.14 GB | 1.63 GB | 1.63 GB |
| Join input rows, summed | 108.41 M | 9.73 M | 9.68 M |
| Network bytes, summed | 1,351.80 MB | 127.53 MB | 88.12 MB |
| Join compute, summed | 5,154 ms | 1,378 ms | 609 ms |
| Parquet predicate evaluation, summed | 1.6 ms | 430 ms | 483 ms |
| Remote updates | 0 | 214.6 | 80.3 |
| Tasks | 106 | 106 | 55 |

- The remote CollectLeft filters are doing useful work: scans emit about
  89% fewer rows, join inputs fall about 91%, and shuffle traffic falls
  about 91%. The scans are at least as selective as in the local run.
  This is not a failure to propagate the filters.
- Remote execution still reads 1.63 GB, has 106 tasks rather than 55,
  and performs more post-filter join/network work than locally. The
  disabled query already takes about 1.3 seconds on this larger cluster.
  Work reduction is not the same as reducing its end-to-end bottleneck.
- The measurements do not isolate whether scheduling, S3 latency,
  cross-node exchange, reporting, or other overhead dominates that
  bottleneck. There are no spills. B records zero global-hash merges for
  Q80, so its slower result is not evidence that a larger CASE caused it.

See the [disabled plan][q80-control], [A-on plan][q80-a], and the
[local/remote counters](local-vs-remote.md#q80-a-on).

## Other Local Wins That Did Not Transfer

The [scan-cost investigation](sf10-scan-cost-investigation.md) examines
predicate evaluation, file-opening overhead, and between-pass variation
in more detail, separating measured evidence from the working theory.

### Q27: Rows Fall, Read Volume Does Not

Local A-on was 2.41x faster. Remotely, A-on is 2,393.2 ms versus a
2,179.6 ms matching control, or 0.91x. Both enabled blocks lose.

- Scan output falls from 86.79 M to 5.70 M rows and shuffle traffic
  from 1,521.41 to 140.40 MB. Filtering is effective.
- Scanned bytes do not fall: 4.563 GB becomes 4.572 GB. Predicate
  evaluation rises from 40 ms to 4,851 ms summed across partitions;
  the local enabled value was 1,178 ms. This trades downstream work
  for substantial scan-side computation without an I/O reduction.
- The remote plan has 96 tasks versus 40 locally. These counters explain
  why selectivity alone is insufficient; they do not establish a precise
  wall-time breakdown. See the [disabled][q27-control] and
  [enabled][q27-a] plans.

### Q25, Q17, And Q98

- Q25 A-on was 2.37x locally, but is 0.88x remotely. Remote scan output
  falls from 46.18 M to 2.11 M and shuffle traffic from 1,052.49 to
  120.87 MB. It still reads about 789 MB and adds 1,339 ms of summed
  predicate evaluation. Q25 A-off's 1.09x mean gain is not consistent
  across passes, so it is not a confirmed win.
- Q17 B-on was 2.05x locally, but is 0.71x remotely. Scan output falls
  from 46.18 M to 2.87 M, but scan bytes only fall from 353 to 306 MB,
  while summed predicate evaluation rises from 2.9 to 1,142 ms. The
  empty result does not mean the joins/scans avoided all work.
- Q98 B-on was 1.97x locally, but is 0.82x remotely. Its local-only
  filters reduce scan output from 28.83 M to 0.109 M rows, yet read
  volume stays about 555-558 MB and exchange volume stays about 47 MB.
  The avoided join work does not yield a repeated end-to-end gain.
  Between-pass variance is large; no outliers have been discarded.

Their [local/remote metric tables](local-vs-remote.md) link both plans
for each environment. These are examples of real filtering without a
replicated latency benefit, not evidence that the scans ignored updates.

### Predicate Shape Also Changes With Parallelism

Q26's partitioned producer uses four tasks and four local hash partitions
in the local plan, versus twelve tasks and fifteen local partitions in
the remote plan. B's merged CASE consequently covers 16 versus 180
global buckets. Both plans and the [merge source][merge-source] support
that structural difference.

For B-on, remote scans emit fewer rows than locally, 1.85 M versus
2.99 M, yet summed Parquet predicate evaluation is 782 ms versus 31 ms.
More selective is not automatically cheaper. The larger expression is a
plausible cost contributor, not an isolated proof of the 25x evaluation
time difference; CPU, batching, placement, and timing also differ.

## Evidence And Limitations

The frozen remote endpoint applies completed metric reports but does not
rewrite completed dynamic-filter snapshots before printing the plan.
`DynamicFilter [ empty ]` can therefore appear on a scan that did prune
rows. Saved plans are unmodified. Attribution uses producer/consumer
placement and scan, join, network, and update counters together.
See [the route audit](filter-routes.md).

Operator times are summed across tasks and partitions, not query wall
time or a critical-path decomposition. Scan and network byte metrics
measure different work. Fewer returned rows do not necessarily mean
fewer bytes read, and fewer bytes shuffled need not reduce the bottleneck.

The dynamic-filter switch controls local and remote filtering together.
A mixed query is not an ablation of remote filtering alone. Nor does
having a partitioned join isolate its filter from CollectLeft predicates.

All samples, including slow outliers, stay in the arithmetic means.
The repeated-win rule requires at least a 10% gain, both enabled block
means below both control block means, a within-block bootstrap lower
bound above 1.0, and visible work reduction. These are exploratory
intervals, not guarantees about another day or multiple-comparison-adjusted
claims. The ten queries were selected because they won locally; this is
not a full-suite performance claim.

Remote result row counts are checked, not complete result values. Q17
returns zero rows at SF10 and is not a good presentation example. The
local confirmations performed stronger result-value checks, with their
documented tolerances and LIMIT-tie handling.

## Why The Run Took Time

The original schedule waited for both datasets before starting queries.
SF10 generation took about six minutes; SF100 generation then took about
43 minutes. The first SF10 pass also hit an expired SSO token before
moving to SF100, causing roughly 13 minutes of auth delay. The original
interleaved schedule then spent about 16 minutes on SF100's first pass.

After the priority change, scheduling became SF10-only and retained all
completed measurements. The deliberate handoff was between query blocks;
its two manifest error entries are operational bookkeeping, not failed
query samples. Fresh deployments, table registration, configuration checks,
health checks, and report generation add orchestration time outside the
reported query latencies. See [the schedule](plan.md) and
[manifest](manifest.json).

[q26-control]: plans/tpcds-sf10-q26-Control-off.txt
[q26-a]: plans/tpcds-sf10-q26-A-off.txt
[q26-b]: plans/tpcds-sf10-q26-B-off.txt
[q80-control]: plans/tpcds-sf10-q80-Control-on.txt
[q80-a]: plans/tpcds-sf10-q80-A-on.txt
[q27-control]: plans/tpcds-sf10-q27-Control-on.txt
[q27-a]: plans/tpcds-sf10-q27-A-on.txt
[merge-source]: source/partitioned_dynamic_filter.rs
[q21-control]: plans/tpcds-sf10-q21-Control-off.txt
[q21-a]: plans/tpcds-sf10-q21-A-off.txt
[q39-control]: plans/tpcds-sf10-q39-Control-off.txt
[q39-a]: plans/tpcds-sf10-q39-A-off.txt
[q37-control]: plans/tpcds-sf10-q37-Control-on.txt
[q37-a]: plans/tpcds-sf10-q37-A-on.txt
[q82-control]: plans/tpcds-sf10-q82-Control-on.txt
[q82-a]: plans/tpcds-sf10-q82-A-on.txt
