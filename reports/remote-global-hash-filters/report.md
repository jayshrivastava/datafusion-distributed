# Remote Dynamic Filtering Results

**Partial results. Do not draw performance conclusions yet.**

Completed query/case blocks: 153/264.
Benchmark execution is stopped at the user's request for review.
ClickBench is deferred pending review of the complete TPC-H comparison.
Each completed block has one excluded warmup and five measured executions.

[Settings and methodology](plan.md). Full counters: [metrics.json](metrics.json).

[Latency spread and per-pass means](variability.md).

Counters below are means across measured runs and are approximate because
the plan display rounds them. Representative plans are closest to the median.
They are executed plans with per-task metrics, not optimizer estimates.

[Interpretation and operator-level evidence](findings.md).

After the pass-2 interruption, concurrent bot runs on separate nodes were
explicitly allowed. Node separation and worker health remain checked.
The first pass had no competing workers; resumed runs record concurrent
workers per sample. Shared S3/network effects remain a comparison caveat.

## tpch/sf100

### Suite Totals

| Case | Queries | Sum means ms | Sum medians ms |
| --- | ---: | ---: | ---: |
| Control-off | 22/22 | 56658.3 | 55738.8 |
| Control-on | 22/22 | 60636.5 | 59912.0 |
| A-off | 22/22 | 57007.9 | 56271.0 |
| A-on | 22/22 | 96826.8 | 96685.0 |
| B-off | 22/22 | 59094.2 | 57666.3 |
| B-on | 22/22 | 117449.7 | 116545.8 |

### Parquet Pushdown And Reordering off

Arithmetic mean milliseconds; ratios compare against the no-filter control.

| Query | No DF | A | B | No DF/A | No DF/B | A/B |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| q1 | 1790.3 | 1820.6 | 1947.0 | 0.98 | 0.92 | 0.94 |
| q2 | 2088.9 | 2027.3 | 1674.6 | 1.03 | 1.25 | 1.21 |
| q3 | 2276.0 | 2368.4 | 2445.0 | 0.96 | 0.93 | 0.97 |
| q4 | 1668.3 | 1168.6 | 1269.5 | 1.43 | 1.31 | 0.92 |
| q5 | 3551.3 | 2988.6 | 3362.3 | 1.19 | 1.06 | 0.89 |
| q6 | 1187.6 | 1209.9 | 1081.3 | 0.98 | 1.10 | 1.12 |
| q7 | 3531.6 | 3537.6 | 4516.6 | 1.00 | 0.78 | 0.78 |
| q8 | 4042.5 | 3976.7 | 3779.6 | 1.02 | 1.07 | 1.05 |
| q9 | 4697.0 | 4871.9 | 4908.6 | 0.96 | 0.96 | 0.99 |
| q10 | 4857.8 | 5020.3 | 4929.2 | 0.97 | 0.99 | 1.02 |
| q11 | 1716.2 | 1851.1 | 1018.4 | 0.93 | 1.69 | 1.82 |
| q12 | 1460.0 | 1480.3 | 1476.5 | 0.99 | 0.99 | 1.00 |
| q13 | 1154.8 | 1085.3 | 1184.7 | 1.06 | 0.97 | 0.92 |
| q14 | 1374.7 | 1332.8 | 1518.1 | 1.03 | 0.91 | 0.88 |
| q15 | 3183.8 | 3026.3 | 5024.9 | 1.05 | 0.63 | 0.60 |
| q16 | 1054.8 | 1016.3 | 993.5 | 1.04 | 1.06 | 1.02 |
| q17 | 3482.5 | 4285.9 | 4258.6 | 0.81 | 0.82 | 1.01 |
| q18 | 3712.6 | 3785.6 | 3907.5 | 0.98 | 0.95 | 0.97 |
| q19 | 1498.5 | 1646.6 | 1590.6 | 0.91 | 0.94 | 1.04 |
| q20 | 2325.3 | 2517.4 | 2254.6 | 0.92 | 1.03 | 1.12 |
| q21 | 5231.2 | 5172.8 | 5177.9 | 1.01 | 1.01 | 1.00 |
| q22 | 772.8 | 817.7 | 775.1 | 0.95 | 1.00 | 1.06 |

### Parquet Pushdown And Reordering on

Arithmetic mean milliseconds; ratios compare against the no-filter control.

| Query | No DF | A | B | No DF/A | No DF/B | A/B |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| q1 | 1928.7 | 2143.9 | 2033.0 | 0.90 | 0.95 | 1.05 |
| q2 | 1773.2 | 2370.4 | 3423.1 | 0.75 | 0.52 | 0.69 |
| q3 | 3075.2 | 3737.9 | 5890.9 | 0.82 | 0.52 | 0.63 |
| q4 | 1508.6 | 2760.3 | 4161.5 | 0.55 | 0.36 | 0.66 |
| q5 | 3173.2 | 4026.1 | 6163.5 | 0.79 | 0.51 | 0.65 |
| q6 | 2160.0 | 2256.5 | 2003.7 | 0.96 | 1.08 | 1.13 |
| q7 | 3714.5 | 4757.6 | 7865.3 | 0.78 | 0.47 | 0.60 |
| q8 | 4285.9 | 7429.0 | 13455.3 | 0.58 | 0.32 | 0.55 |
| q9 | 4808.2 | 8653.5 | 17538.1 | 0.56 | 0.27 | 0.49 |
| q10 | 5036.0 | 5710.7 | 6577.2 | 0.88 | 0.77 | 0.87 |
| q11 | 1111.6 | 1908.3 | 2136.3 | 0.58 | 0.52 | 0.89 |
| q12 | 2232.6 | 2522.5 | 2728.1 | 0.89 | 0.82 | 0.92 |
| q13 | 1219.5 | 1355.9 | 2066.8 | 0.90 | 0.59 | 0.66 |
| q14 | 1549.0 | 1828.5 | 2325.6 | 0.85 | 0.67 | 0.79 |
| q15 | 3331.5 | 3538.9 | 3522.8 | 0.94 | 0.95 | 1.00 |
| q16 | 1180.4 | 1347.0 | 1632.8 | 0.88 | 0.72 | 0.82 |
| q17 | 3446.7 | 14688.1 | 7051.4 | 0.23 | 0.49 | 2.08 |
| q18 | 3671.8 | 10929.9 | 5645.8 | 0.34 | 0.65 | 1.94 |
| q19 | 2458.6 | 2893.7 | 2817.8 | 0.85 | 0.87 | 1.03 |
| q20 | 2699.7 | 3696.0 | 4721.6 | 0.73 | 0.57 | 0.78 |
| q21 | 5466.4 | 7299.5 | 12260.6 | 0.75 | 0.45 | 0.60 |
| q22 | 805.2 | 972.6 | 1428.6 | 0.83 | 0.56 | 0.68 |

## Evidence For Turning Dynamic Filtering On

Rank by the fastest dynamic-enabled case in either A or B versus Control-off.
Also compare the matching Parquet control to isolate dynamic filtering.
Rankings below require ten measurements in all six cases for that query.
A faster time alone is not proof of pruning or reduced computation.

## Limits

- The dynamic-filter switch controls local and remote filtering together.
- Pruned-row counters include static predicates. Compare matched controls.
- Summed operator time is not wall time and may include concurrent work.
- Scan/network row sums count repeated scans and exchange hops separately.
- Row counts are checked; full result values are not returned by this runner.
- Stock timed plans do not replay final worker predicates for display.
- Server timing includes physical planning and metric-plan collection/rendering.
- S3 and runtime variability remain; all samples, including slow ones, remain.
