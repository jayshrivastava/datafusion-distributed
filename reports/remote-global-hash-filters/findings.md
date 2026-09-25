# Does Turning Dynamic Filtering On Help?

**Provisional: 153/264 blocks completed.**

Stopped at the user's request for review. All 22 queries have at least five
measurements in every configuration. Some B cases have ten; the final
A/control pass has not run. No further benchmarks are running.

**Bottom line:** no large latency improvement is yet backed by evidence
that dynamic filtering caused it. Q11, Q4, and Q2 have the largest apparent
speedups but unchanged operator row counts. Q15 has a smaller timing gain
and a demonstrable reduction in downstream work, but its wall-time gain
is not yet distinguished from variation.

The primary comparison is dynamic filtering enabled versus Control-off,
not B versus A. Either implementation can supply a useful winning case.
Control-off disables dynamic filtering, Parquet row pushdown, and reordering.
For each query, rank the fastest of A-off, A-on, B-off, and B-on.

The matched-control ratio holds Parquet settings fixed. For an `on` case,
it compares with Control-on, separating the dynamic-filter contribution
from enabling static Parquet row pushdown and reordering.

These are exploratory best-of-four rankings, not significance tests.
Keep both pass means and plan evidence in view before claiming a benefit.

[Full matrix](report.md), [spread and pass means](variability.md),
[settings](plan.md), and [all parsed counters](metrics.json).

## Ranked Candidates

Means in milliseconds. Ratios above 1 mean faster with dynamic filtering.
`n` is the number of measurements in Control-off / the selected case.

| Query | Best case | Off ms | DF ms | Gain | Matched gain | n |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| q11 | B-off | 1716 | 1018 | 1.69x | 1.69x | 5/5 |
| q4 | A-off | 1668 | 1169 | 1.43x | 1.43x | 5/5 |
| q2 | B-off | 2089 | 1675 | 1.25x | 1.25x | 5/10 |
| q5 | A-off | 3551 | 2989 | 1.19x | 1.19x | 5/5 |
| q6 | B-off | 1188 | 1081 | 1.10x | 1.10x | 5/10 |
| q8 | B-off | 4042 | 3780 | 1.07x | 1.07x | 5/10 |
| q13 | A-off | 1155 | 1085 | 1.06x | 1.06x | 5/5 |
| q16 | B-off | 1055 | 993 | 1.06x | 1.06x | 5/5 |
| q15 | A-off | 3184 | 3026 | 1.05x | 1.05x | 5/5 |
| q14 | A-off | 1375 | 1333 | 1.03x | 1.03x | 5/5 |
| q20 | B-off | 2325 | 2255 | 1.03x | 1.03x | 5/5 |
| q21 | A-off | 5231 | 5173 | 1.01x | 1.01x | 5/5 |
| q7 | A-off | 3532 | 3538 | 1.00x | 1.00x | 5/5 |
| q22 | B-off | 773 | 775 | 1.00x | 1.00x | 5/5 |
| q12 | B-off | 1460 | 1477 | 0.99x | 0.99x | 5/5 |
| q10 | B-off | 4858 | 4929 | 0.99x | 0.99x | 5/5 |
| q1 | A-off | 1790 | 1821 | 0.98x | 0.98x | 5/5 |
| q18 | A-off | 3713 | 3786 | 0.98x | 0.98x | 5/5 |
| q9 | A-off | 4697 | 4872 | 0.96x | 0.96x | 5/5 |
| q3 | A-off | 2276 | 2368 | 0.96x | 0.96x | 5/5 |
| q19 | B-off | 1498 | 1591 | 0.94x | 0.94x | 5/5 |
| q17 | B-off | 3483 | 4259 | 0.82x | 0.82x | 5/10 |

## Candidates At Least 1.20x Faster

These are timing candidates, not automatically attributable filter wins.
Counters compare the selected case with its matching Parquet control.

### Q11: B-off, 1.69x

Plans: [Control-off](plans/tpch-q11-Control-off.txt),
[matching control](plans/tpch-q11-Control-off.txt),
[dynamic enabled](plans/tpch-q11-B-off.txt).

| Metric | Matched control | DF enabled |
| --- | ---: | ---: |
| Scan output rows | 161980050.0 | 161980050.0 |
| Scan GB | 1.0 | 1.0 |
| Dynamic row groups pruned | 0.0 | 0.0 |
| Statistics row groups pruned | 0.0 | 0.0 |
| Parquet rows pruned | 0.0 | 0.0 |
| Parquet evaluation ms | 0.0 | 0.0 |
| FilterExec compute ms | 0.1 | 0.7 |
| Network output rows | 165182696.0 | 165182696.0 |
| Transferred GB | 2.0 | 2.0 |
| Join compute ms | 6505.3 | 6513.6 |
| Spilled GB | 0.0 | 0.0 |
| Remote filter updates | 0.0 | 81.4 |
| Global-hash merges | - | 2.0 |

- Remote updates were received (mean 81.4). That proves
  delivery, not useful pruning or an end-to-end benefit.
- Scan and network output rows are unchanged, with zero dynamically
  pruned row groups. The displayed counters do not explain the speedup.
- Check the two pass means before treating this as repeatable. Shared S3
  variability and selection of the fastest configuration remain caveats.

### Q4: A-off, 1.43x

Plans: [Control-off](plans/tpch-q4-Control-off.txt),
[matching control](plans/tpch-q4-Control-off.txt),
[dynamic enabled](plans/tpch-q4-A-off.txt).

| Metric | Matched control | DF enabled |
| --- | ---: | ---: |
| Scan output rows | 750070000.0 | 750070000.0 |
| Scan GB | 2.9 | 2.9 |
| Dynamic row groups pruned | 0.0 | 0.0 |
| Statistics row groups pruned | 0.0 | 0.0 |
| Parquet rows pruned | 0.0 | 0.0 |
| Parquet evaluation ms | 0.0 | 0.0 |
| FilterExec compute ms | 2790.4 | 2814.8 |
| Network output rows | 385084705.0 | 385084705.0 |
| Transferred GB | 0.9 | 0.9 |
| Join compute ms | 5527.2 | 5503.3 |
| Spilled GB | 0.0 | 0.0 |
| Remote filter updates | 0.0 | 19.8 |
| Global-hash merges | - | - |

- Remote updates were received (mean 19.8). That proves
  delivery, not useful pruning or an end-to-end benefit.
- Scan and network output rows are unchanged, with zero dynamically
  pruned row groups. The displayed counters do not explain the speedup.
- Check the two pass means before treating this as repeatable. Shared S3
  variability and selection of the fastest configuration remain caveats.

### Q2: B-off, 1.25x

Plans: [Control-off](plans/tpch-q2-Control-off.txt),
[matching control](plans/tpch-q2-Control-off.txt),
[dynamic enabled](plans/tpch-q2-B-off.txt).

| Metric | Matched control | DF enabled |
| --- | ---: | ---: |
| Scan output rows | 181980060.0 | 181980060.0 |
| Scan GB | 0.9 | 0.9 |
| Dynamic row groups pruned | 0.0 | 0.0 |
| Statistics row groups pruned | 0.0 | 0.0 |
| Parquet rows pruned | 0.0 | 0.0 |
| Parquet evaluation ms | 0.0 | 0.0 |
| FilterExec compute ms | 78.8 | 77.1 |
| Network output rows | 190329004.0 | 190329004.0 |
| Transferred GB | 2.0 | 2.0 |
| Join compute ms | 7407.8 | 7441.8 |
| Spilled GB | 0.0 | 0.0 |
| Remote filter updates | 0.0 | 140.1 |
| Global-hash merges | - | 3.0 |

- Remote updates were received (mean 140.1). That proves
  delivery, not useful pruning or an end-to-end benefit.
- Scan and network output rows are unchanged, with zero dynamically
  pruned row groups. The displayed counters do not explain the speedup.
- Check the two pass means before treating this as repeatable. Shared S3
  variability and selection of the fastest configuration remain caveats.

## Smaller Gains And Their Evidence

The remaining faster cases also matter. All selected cases have Parquet
row pushdown off. Each has unchanged scan and network output rows versus
Control-off, zero dynamically pruned row groups, and unchanged task counts.
The exceptions to unchanged downstream work are explained below.

| Query | Case | Gain | Plans: control / enabled | Assessment |
| --- | --- | ---: | --- | --- |
| Q5 | A-off | 1.19x | [off][c5] / [on][a5] | Unexplained |
| Q6 | B-off | 1.10x | [off][c6] / [on][b6] | No dynamic filters |
| Q8 | B-off | 1.07x | [off][c8] / [on][b8] | Unexplained |
| Q13 | A-off | 1.06x | [off][c13] / [on][a13] | Unexplained |
| Q16 | B-off | 1.06x | [off][c16] / [on][b16] | Unexplained |
| Q15 | A-off | 1.05x | [off][c15] / [on][a15] | Less sort input |
| Q14 | A-off | 1.03x | [off][c14] / [on][a14] | Unexplained |
| Q20 | B-off | 1.03x | [off][c20] / [on][b20] | Unexplained |
| Q21 | A-off | 1.01x | [off][c21] / [on][a21] | Unexplained |

- Q5's mean improvement shrinks to 1.05x using medians, 3151 versus
  3014 ms. One slow control sample is 5452 ms. Join compute barely changes,
  from 29.82 to 29.58 summed seconds; this is not persuasive filter evidence.
- Q6 reports no dynamic filters or remote updates. Its B-off pass means
  move from 1198 to 965 ms despite unchanged work. This demonstrates that
  timing gains of this size can occur without any dynamic-filter benefit.
- For Q8, Q13, Q16, Q14, Q20, and Q21, stage/operator output-row comparisons
  show no material reduction. Their small timing differences remain
  unexplained rather than being attributed to filtering.

### Q15: Real Work Reduction, Small Unproven Latency Gain

Mean: 3184 ms Control-off versus 3026 ms A-off, or 1.05x.
Median: 3153 versus 3058 ms, or 1.03x. There are five samples per case,
and their ranges overlap: 2773-3836 versus 2824-3147 ms.

Inspect Stage 6 in the [control plan][c15] and [enabled plan][a15].
The natural FilterExec consumes a dynamic predicate above the supplier/revenue
join and below SortExec. This is not the injected post-scan experiment.

| Stage 6 metric | Control-off | A-off |
| --- | ---: | ---: |
| Inner join output rows, approximately | 999,990 | 999,990 |
| Dynamic FilterExec output rows | Absent | 1 |
| SortExec output rows | 999,990 | 1 |
| SortExec reported compute, summed ms | 1.427 | 0.006 |
| Dynamic FilterExec compute, summed ms | Absent | 7.358 |

The predicate really removes rows before the sort and outer join. However,
scan rows, shuffle rows, and tasks stay unchanged. These counters establish
useful filtering, not that it caused the entire 158 ms mean latency saving.
B applies the same downstream reduction but does not beat Control-off.

## Q17 And Q18: Filtering Without A Latency Win

Improving B relative to A is not the goal of this ranking. Their all-on
cases must also beat filtering disabled to qualify as end-to-end wins.

| Query | Control-off ms | A-on ms | B-on ms |
| --- | ---: | ---: | ---: |
| q17 | 3482.5 | 14688.1 | 7051.4 |
| q18 | 3712.6 | 10929.9 | 5645.8 |

Both reduce substantial downstream work when row pushdown is on, but the
current all-on times remain slower than Control-off. Cheaper predicate
evaluation in B does not, by itself, make dynamic filtering worthwhile.

The following metrics compare against Control-on, keeping Parquet settings
fixed. They therefore do not mistake static Parquet filtering for a dynamic
filter benefit. Times are summed operator seconds, not wall time.

| Query and metric | Control-on | A-on | B-on |
| --- | ---: | ---: | ---: |
| Q17 scan output, million rows | 1200.120 | 707.650 | 712.334 |
| Q17 transferred GB | 11.839 | 5.862 | 5.907 |
| Q17 join compute, seconds | 62.50 | 12.72 | 12.37 |
| Q17 Parquet evaluation, seconds | 0.06 | 604.51 | 193.41 |
| Q18 scan output, million rows | 1365.120 | 615.101 | 622.009 |
| Q18 transferred GB | 10.856 | 1.305 | 1.336 |
| Q18 join compute, seconds | 186.10 | 44.74 | 43.74 |
| Q18 Parquet evaluation, seconds | 0.00 | 758.10 | 292.61 |

- Q17: Stage 2 lineitem output falls from 600.05 million rows to 111.14
  million with A or 113.31 million with B. The other lineitem scan remains
  mostly unfiltered. The evaluation cost outweighs the downstream savings.
- Q18: Stage 3 orders drops from 150.02 million rows to roughly 6398.
  Stage 5 lineitem drops from 600.05 million to 44,790 with A or 6.95
  million on average with B. One B execution forwarded 69.12 million,
  showing update-arrival variability. These are real filtering effects.
- Neither query prunes any dynamic row groups. Filters reduce row and
  network work but fail to deliver a wall-time win in either implementation.

Plans: Q17 [control][c17on], [A][a17on], [B][b17on];
Q18 [control][c18on], [A][a18on], [B][b18on].

### Why The Local Comparison Looked Better

The saved local experiment used **SF10**, not SF100. See its
[setup](../tpch-sf10-global-hash-filters/README.md) and
[results](../tpch-sf10-global-hash-filters/report.md).

| Local SF10 query | B all-off ms | B all-on ms | Off / on |
| --- | ---: | ---: | ---: |
| Q17 | 1777.5 | 2009.2 | 0.88x |
| Q18 | 2044.2 | 1328.1 | 1.54x |

Q17's quoted 1.67x local gain was B versus A, not filtering on versus off.
Q18 really did improve over all-off locally. That result is specific to
four workers with four partitions each, warm local files, and shared ARM
cores. Remote runs use SF100 on twelve 15-CPU x86 workers reading S3.

The global CASE can grow from 16 to 180 branches. DataFusion's general CASE
evaluator walks branches and filters intermediate batches; it is not a
constant-time bucket lookup. This is a plausible scaling cost, not a
CPU-profile-proven attribution. Q18's measured B Parquet evaluation rises
from 4.89 summed seconds locally to 292.61 remotely, while data grows 10x.
The balance between predicate cost and avoided join/shuffle work changes.

Local dynamic-only runs also injected post-scan filters; remote runs do not.
That difference does not explain the local all-on win: the post-scan
fallback is disabled whenever Parquet row pushdown is enabled. Local timing
also excludes planning and plan rendering, unlike the remote harness.

## Interpretation And Limits

- `off` variants leave Parquet row-group pruning enabled. Disabling row
  pushdown does not disable row-group pruning.
- Neither build injects post-scan FilterExec nodes. Natural filters remain.
- Dynamic filtering controls both local and remote filters in this matrix.
- Plan counters are rounded; operator times sum concurrent task work.
- Row counts match; the remote runner does not compare full result values.
- The timer includes physical planning and metric-plan collection/rendering.
- Both interruptions were checkpointed. Unvalidated Q8 and Q3 blocks were
  archived and rerun after concurrency approval and SSO refresh respectively.
- Concurrent bot runs use separate nodes. Worker health and node separation
  are checked after every block; shared S3 effects remain possible.
- The incomplete Q9 B-off repeat was archived on the user's stop request
  and contributes no measurements. ClickBench was not run.

[c5]: plans/tpch-q5-Control-off.txt
[a5]: plans/tpch-q5-A-off.txt
[c6]: plans/tpch-q6-Control-off.txt
[b6]: plans/tpch-q6-B-off.txt
[c8]: plans/tpch-q8-Control-off.txt
[b8]: plans/tpch-q8-B-off.txt
[c13]: plans/tpch-q13-Control-off.txt
[a13]: plans/tpch-q13-A-off.txt
[c16]: plans/tpch-q16-Control-off.txt
[b16]: plans/tpch-q16-B-off.txt
[c15]: plans/tpch-q15-Control-off.txt
[a15]: plans/tpch-q15-A-off.txt
[c14]: plans/tpch-q14-Control-off.txt
[a14]: plans/tpch-q14-A-off.txt
[c20]: plans/tpch-q20-Control-off.txt
[b20]: plans/tpch-q20-B-off.txt
[c21]: plans/tpch-q21-Control-off.txt
[a21]: plans/tpch-q21-A-off.txt
[c17on]: plans/tpch-q17-Control-on.txt
[a17on]: plans/tpch-q17-A-on.txt
[b17on]: plans/tpch-q17-B-on.txt
[c18on]: plans/tpch-q18-Control-on.txt
[a18on]: plans/tpch-q18-A-on.txt
[b18on]: plans/tpch-q18-B-on.txt
