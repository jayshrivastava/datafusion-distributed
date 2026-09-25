# Remote TPC-DS Dynamic-Filter Findings

Status: **sf10_complete_sf100_paused_for_review**. Updated 2026-09-25T15:41:40.234Z.

800/1,200 measured executions are recorded.

tpcds/sf10: 600/600 measured executions; 60/60 complete cases.
tpcds/sf100: 200/600 measured executions; 0/60 complete cases.

Start with the [SF10 report](sf10-report.md) for the interpretation and
[local-versus-remote comparison](local-vs-remote.md). SF100 is separate.

Data preparation for tpcds/sf10: complete.
Data preparation for tpcds/sf100: complete.

This report is separate from the local benchmark. The remote endpoint
times planning, execution, metric collection, and plan rendering together.
Do not directly compare these latencies with local execution-only timings.

See the [full matrix](matrix.md), [plan](plan.md), and
[raw summary](summary.json). Incomplete cases are not final results.

[Filter routes](filter-routes.md) records local/remote producers from
the executed remote plans. A listed relationship is not a separate
attribution of the latency gain to that producer type.

## Configuration

Workers: 12, c5n.4xlarge;
15 CPU and 38Gi per pod on distinct nodes.

Control disables dynamic filters. A uses the existing join-filter OR merge;
B adds global-hash CASE merging. Both exclude injected post-scan FilterExecs.
`off`/`on` means Parquet row pushdown and reordering both off/on. Statistics
pruning remains enabled. Other options retain the recorded runtime defaults.

Each complete case has ten measurements across two five-run passes, with
one excluded warmup per pass. Result row counts are checked across cases;
the stock endpoint does not return result values for full equality checks.

## Best Dynamic-Filter Case Per Query

Ranked against the disabled control with matching Parquet settings. Also
show the gain against the faster disabled Parquet configuration. Select
the case with the largest matched-control gain, which need not be the
lowest-latency case in the full matrix.

`Repeated` requires at least a 10% mean gain, a bootstrap lower bound
above 1.0, both enabled block means faster than either control block,
and a visible dynamic consumer with at least 5% less measured work.

| Dataset | Query | Case | Remote | Local | DF off ms | DF on ms | Gain | Best off | Repeated |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| tpcds/sf10 | Q21 | A-off | None | Join | 930.1 | 280.4 | 3.32x | 3.17x | Yes |
| tpcds/sf10 | Q37 | A-on | Join, TopK | Join | 1,720.7 | 717.5 | 2.40x | 1.32x | Yes |
| tpcds/sf10 | Q39 | A-off | None | Join | 1,840.3 | 777.0 | 2.37x | 2.37x | Yes |
| tpcds/sf10 | Q82 | A-off | Join, TopK | Join | 1,060.7 | 521.3 | 2.03x | 2.03x | Yes |
| tpcds/sf10 | Q26 | B-on | Join, TopK | None | 1,931.6 | 1,278.5 | 1.51x | 0.82x | Yes |
| tpcds/sf10 | Q25 | A-off | Join | None | 1,586.5 | 1,449.2 | 1.09x | 1.05x | No |
| tpcds/sf10 | Q98 | B-off | None | Join | 739.1 | 757.9 | 0.98x | 0.98x | No |
| tpcds/sf10 | Q27 | A-off | Join, TopK | None | 1,677.5 | 1,728.0 | 0.97x | 0.97x | No |
| tpcds/sf10 | Q80 | A-off | Join | None | 1,300.2 | 1,362.5 | 0.95x | 0.95x | No |
| tpcds/sf10 | Q17 | A-off | Join | None | 1,095.8 | 1,256.4 | 0.87x | 0.87x | No |

## Provisional Comparisons

These entries are incomplete. They are useful for inspecting progress,
not for claiming repeatability or selecting a final winner. The sample
count is disabled/enabled; all measured alternatives are in the matrix.

| Dataset | Query | Case | n | DF off ms | DF on ms | Gain | Work reduced |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| tpcds/sf100 | Q21 | A-off | 5/5 | 1,066.1 | 335.8 | 3.18x | Yes |
| tpcds/sf100 | Q39 | A-on | 5/5 | 3,728.4 | 1,579.8 | 2.36x | Yes |
| tpcds/sf100 | Q82 | A-on | 5/5 | 3,986.5 | 1,849.7 | 2.16x | Yes |
| tpcds/sf100 | Q37 | A-on | 5/5 | 1,707.5 | 913.0 | 1.87x | Yes |
| tpcds/sf100 | Q26 | A-off | 5/5 | 2,064.9 | 1,467.2 | 1.41x | Yes |
| tpcds/sf100 | Q27 | A-on | 5/5 | 6,243.7 | 4,510.9 | 1.38x | Yes |
| tpcds/sf100 | Q80 | A-on | 5/5 | 4,411.9 | 3,516.8 | 1.25x | Yes |
| tpcds/sf100 | Q17 | A-off | 5/5 | 3,329.8 | 3,046.4 | 1.09x | Yes |
| tpcds/sf100 | Q25 | A-on | 5/5 | 4,303.8 | 4,451.6 | 0.97x | Yes |
| tpcds/sf100 | Q98 | A-off | 5/5 | 1,570.7 | 1,712.1 | 0.92x | Not shown |

## Plan-Metric Evidence

Counters are means across all measured plans for each case. Text-plan
counters are rounded by DataFusion; treat derived totals as approximate.
Join and network counters sum operator work, not unique source rows.
Operator times sum across partitions; they are not query wall time.

### tpcds/sf10 Q21: A-off

Plans: [disabled](plans/tpcds-sf10-q21-Control-off.txt),
 [enabled](plans/tpcds-sf10-q21-A-off.txt).

Mean latency: 930.1 -> 280.4 ms (3.32x).
Best-disabled comparison: 3.17x.

Within-pass bootstrap interval: 2.84x - 3.87x.
Fastest-control-block / slowest-enabled-block: 2.93x.

| Metric | Disabled | Enabled |
| --- | ---: | ---: |
| Scan output rows | 133,295,060.0 | 4,975,060.0 |
| Scan bytes | 463,110,000.0 | 26,114,000.0 |
| Statistics row groups pruned | 0.0 | 112.0 |
| Dynamic row groups pruned | 0.0 | 0.0 |
| Page-index rows pruned | 0.0 | 11,980,000.0 |
| Row-pushdown rejects | 0.0 | 0.0 |
| Predicate evaluation ms | 0.0 | 0.0 |
| Join input rows | 274,633,062.0 | 9,916,662.0 |
| Network bytes | 2,334,289.9 | 2,551,203.8 |
| Spilled bytes | 0.0 | 0.0 |
| Remote filter updates | 0.0 | 0.0 |
| Global-hash merges | - | - |

| Table | Scan rows off | Scan rows on | Scan bytes off | Scan bytes on |
| --- | ---: | ---: | ---: | ---: |
| inventory | 133,120,000.0 | 4,800,000.0 | 458,200,000.0 | 21,204,000.0 |
| item | 102,000.0 | 102,000.0 | 3,790,000.0 | 3,790,000.0 |

### tpcds/sf10 Q37: A-on

Plans: [disabled](plans/tpcds-sf10-q37-Control-on.txt),
 [enabled](plans/tpcds-sf10-q37-A-on.txt).

Mean latency: 1,720.7 -> 717.5 ms (2.40x).
Best-disabled comparison: 1.32x.

Within-pass bootstrap interval: 2.23x - 2.58x.
Fastest-control-block / slowest-enabled-block: 2.33x.

| Metric | Disabled | Enabled |
| --- | ---: | ---: |
| Scan output rows | 65,060,866.0 | 676.0 |
| Scan bytes | 506,855,800.0 | 70,411,800.0 |
| Statistics row groups pruned | 0.0 | 112.0 |
| Dynamic row groups pruned | 0.0 | 0.0 |
| Page-index rows pruned | 0.0 | 11,980,000.0 |
| Row-pushdown rejects | 82,624,990.0 | 19,375,090.0 |
| Predicate evaluation ms | 291.2 | 101.6 |
| Join input rows | 50,829,026.0 | 964.0 |
| Network bytes | 7,815,864.3 | 721,449.0 |
| Spilled bytes | 0.0 | 0.0 |
| Remote filter updates | 0.0 | 31.4 |
| Global-hash merges | - | - |

| Table | Scan rows off | Scan rows on | Scan bytes off | Scan bytes on |
| --- | ---: | ---: | ---: | ---: |
| inventory | 50,660,000.0 | 74.0 | 459,880,000.0 | 21,872,000.0 |
| catalog_sales | 14,400,800.0 | 536.0 | 42,700,000.0 | 43,304,000.0 |

### tpcds/sf10 Q39: A-off

Plans: [disabled](plans/tpcds-sf10-q39-Control-off.txt),
 [enabled](plans/tpcds-sf10-q39-A-off.txt).

Mean latency: 1,840.3 -> 777.0 ms (2.37x).
Best-disabled comparison: 2.37x.

Within-pass bootstrap interval: 2.23x - 2.52x.
Fastest-control-block / slowest-enabled-block: 2.16x.

| Metric | Disabled | Enabled |
| --- | ---: | ---: |
| Scan output rows | 266,590,120.0 | 4,970,120.0 |
| Scan bytes | 919,793,200.0 | 32,648,200.0 |
| Statistics row groups pruned | 0.0 | 208.0 |
| Dynamic row groups pruned | 0.0 | 0.0 |
| Page-index rows pruned | 0.0 | 45,720,000.0 |
| Row-pushdown rejects | 0.0 | 0.0 |
| Predicate evaluation ms | 0.0 | 0.0 |
| Join input rows | 799,199,468.0 | 14,339,468.0 |
| Network bytes | 58,716,600.3 | 56,954,030.1 |
| Spilled bytes | 0.0 | 0.0 |
| Remote filter updates | 0.0 | 0.0 |
| Global-hash merges | - | - |

| Table | Scan rows off | Scan rows on | Scan bytes off | Scan bytes on |
| --- | ---: | ---: | ---: | ---: |
| inventory | 266,240,000.0 | 4,620,000.0 | 916,400,000.0 | 29,255,000.0 |
| date_dim | 146,100.0 | 146,100.0 | 1,427,600.0 | 1,427,600.0 |
| item | 204,000.0 | 204,000.0 | 1,965,600.0 | 1,965,600.0 |

### tpcds/sf10 Q82: A-off

Plans: [disabled](plans/tpcds-sf10-q82-Control-off.txt),
 [enabled](plans/tpcds-sf10-q82-A-off.txt).

Mean latency: 1,060.7 -> 521.3 ms (2.03x).
Best-disabled comparison: 2.03x.

Within-pass bootstrap interval: 1.84x - 2.26x.
Fastest-control-block / slowest-enabled-block: 1.79x.

| Metric | Disabled | Enabled |
| --- | ---: | ---: |
| Scan output rows | 162,095,050.0 | 33,915,050.0 |
| Scan bytes | 560,380,000.0 | 126,040,000.0 |
| Statistics row groups pruned | 0.0 | 96.0 |
| Dynamic row groups pruned | 0.0 | 0.0 |
| Page-index rows pruned | 0.0 | 28,620,000.0 |
| Row-pushdown rejects | 0.0 | 0.0 |
| Predicate evaluation ms | 0.0 | 0.0 |
| Join input rows | 51,646,352.0 | 2,857,125.0 |
| Network bytes | 35,764,336.6 | 35,842,283.5 |
| Spilled bytes | 0.0 | 0.0 |
| Remote filter updates | 0.0 | 31.6 |
| Global-hash merges | - | - |

| Table | Scan rows off | Scan rows on | Scan bytes off | Scan bytes on |
| --- | ---: | ---: | ---: | ---: |
| inventory | 133,120,000.0 | 4,940,000.0 | 460,400,000.0 | 26,060,000.0 |
| item | 102,000.0 | 102,000.0 | 12,180,000.0 | 12,180,000.0 |
| store_sales | 28,800,000.0 | 28,800,000.0 | 86,680,000.0 | 86,680,000.0 |

### tpcds/sf10 Q26: B-on

Plans: [disabled](plans/tpcds-sf10-q26-Control-on.txt),
 [enabled](plans/tpcds-sf10-q26-B-on.txt).

Mean latency: 1,931.6 -> 1,278.5 ms (1.51x).
Best-disabled comparison: 0.82x.

Within-pass bootstrap interval: 1.37x - 1.66x.
Fastest-control-block / slowest-enabled-block: 1.17x.

| Metric | Disabled | Enabled |
| --- | ---: | ---: |
| Scan output rows | 14,531,104.0 | 1,853,004.0 |
| Scan bytes | 812,722,510.0 | 221,024,510.0 |
| Statistics row groups pruned | 0.0 | 0.0 |
| Dynamic row groups pruned | 0.0 | 0.0 |
| Page-index rows pruned | 0.0 | 11,200,800.0 |
| Row-pushdown rejects | 1,962,682.0 | 3,439,852.0 |
| Predicate evaluation ms | 11.7 | 782.1 |
| Join input rows | 15,955,318.0 | 3,104,548.0 |
| Network bytes | 294,135,920.6 | 56,970,936.3 |
| Spilled bytes | 0.0 | 0.0 |
| Remote filter updates | 0.0 | 103.3 |
| Global-hash merges | - | 1.0 |

| Table | Scan rows off | Scan rows on | Scan bytes off | Scan bytes on |
| --- | ---: | ---: | ---: | ---: |
| item | 102,000.0 | 102,000.0 | 2,160,000.0 | 2,160,000.0 |
| catalog_sales | 14,400,800.0 | 1,722,700.0 | 789,850,000.0 | 198,152,000.0 |

### tpcds/sf10 Q25: A-off

Plans: [disabled](plans/tpcds-sf10-q25-Control-off.txt),
 [enabled](plans/tpcds-sf10-q25-A-off.txt).

Mean latency: 1,586.5 -> 1,449.2 ms (1.09x).
Best-disabled comparison: 1.05x.

Within-pass bootstrap interval: 0.99x - 1.21x.
Fastest-control-block / slowest-enabled-block: 0.97x.

| Metric | Disabled | Enabled |
| --- | ---: | ---: |
| Scan output rows | 46,399,252.0 | 33,598,452.0 |
| Scan bytes | 1,030,065,940.0 | 788,925,940.0 |
| Statistics row groups pruned | 0.0 | 0.0 |
| Dynamic row groups pruned | 0.0 | 0.0 |
| Page-index rows pruned | 0.0 | 12,800,800.0 |
| Row-pushdown rejects | 0.0 | 0.0 |
| Predicate evaluation ms | 0.0 | 0.0 |
| Join input rows | 49,400,082.0 | 36,599,413.0 |
| Network bytes | 1,052,532,162.6 | 874,829,844.5 |
| Spilled bytes | 0.0 | 0.0 |
| Remote filter updates | 0.0 | 138.2 |
| Global-hash merges | - | - |

| Table | Scan rows off | Scan rows on | Scan bytes off | Scan bytes on |
| --- | ---: | ---: | ---: | ---: |
| item | 102,000.0 | 102,000.0 | 10,300,000.0 | 10,300,000.0 |
| date_dim | 219,150.0 | 219,150.0 | 2,141,400.0 | 2,141,400.0 |
| catalog_sales | 14,400,800.0 | 1,600,000.0 | 301,020,000.0 | 56,857,000.0 |
| store_returns | 2,877,200.0 | 2,877,200.0 | 104,620,000.0 | 104,736,000.0 |
| store_sales | 28,800,000.0 | 28,800,000.0 | 611,980,000.0 | 614,887,000.0 |

## Failures And Caveats

- All configurations use the same dataset per scale factor.
- No claim about full result equality follows from matching row counts.
- Filter presence or update counts alone do not prove useful pruning.
- The frozen remote endpoint renders metrics but does not apply the
  completed dynamic-filter snapshots for display. `DynamicFilter [ empty ]`
  can appear on a consumer that actually pruned rows. These saved plans
  are unmodified; attribution uses work counters, not the empty label.
- Full-suite performance cannot be inferred from these selected queries.
- Final predicates may arrive after most scanning has already finished.
- Bootstrap intervals resample independently within each five-run block.
  They describe within-block variation, not day-to-day AWS variability.
  They are exploratory, with no multiple-comparison correction.
- A uses controls measured in the same passes. B uses A controls in the
  surrounding passes; there is no separately measured B-disabled cell.

- 2026-09-25T14:44:38.577Z:
  CredentialsProviderError: Token is expired. To refresh this SSO session run 'aws sso login' with the corresponding profile.

- 2026-09-25T15:20:07.067Z:
  Deliberate SF10-first schedule handoff, not a query failure. See plan.md.

- 2026-09-25T15:20:07.099Z:
  Deliberate SF10-first schedule handoff, not a query failure. See plan.md.
