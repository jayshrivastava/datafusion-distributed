# Q80 / Q27 Scan Probe Measurements

Run status: `complete`; 80 measured executions captured.

Parquet row pushdown and filter reordering are on in every entry.

## Endpoint Latency

Includes plan collection and rendering. This is not execution-only latency.

| Workers | Query | N per case | Filters off | Filters on | Speedup | Paired 95% interval |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 12 | q80 | 20 | 1,550.0 ms | 1,536.0 ms | 1.01x | 0.96 - 1.06x |
| 12 | q27 | 20 | 1,977.8 ms | 1,945.4 ms | 1.02x | 0.98 - 1.05x |

Intervals resample off/on pairs within each fresh-pod session. They
describe observed run variation, not uncertainty across all hardware or days.

## Execution Latency

| Query | DF off (ms) | DF on (ms) | Off/on speedup | Paired 95% interval |
| --- | ---: | ---: | ---: | --- |
| q80 | 838.9 | 981.5 | 0.855x | 0.797 - 0.920x |
| q27 | 1,263.5 | 1,439.5 | 0.878x | 0.830 - 0.921x |

## Timing Breakdown

| Query / case | Planning | First result | Execution | Metrics | Rendering |
| --- | ---: | ---: | ---: | ---: | ---: |
| [q80 Control-on](plans/12w-q80-Control-on.txt) | 26.4 | 803.4 | 838.9 | 236.4 | 448.3 |
| [q80 A-on](plans/12w-q80-A-on.txt) | 26.9 | 943.1 | 981.5 | 240.6 | 287.0 |
| [q27 Control-on](plans/12w-q27-Control-on.txt) | 22.6 | 1,229.6 | 1,263.5 | 228.5 | 463.1 |
| [q27 A-on](plans/12w-q27-A-on.txt) | 23.7 | 1,395.0 | 1,439.5 | 226.0 | 256.2 |

All times are milliseconds. Planning is physical planning; initial SQL
parsing and logical planning precede the endpoint timer. Execution includes
waiting for output EOS and any work the output stream must join.

## Session Means

| Workers | Query / case | Session means (ms) |
| ---: | --- | --- |
| 12 | q80 Control-on | S1: 1,547.9; S2: 1,552.0 |
| 12 | q80 A-on | S1: 1,491.2; S2: 1,580.9 |
| 12 | q27 Control-on | S1: 2,033.7; S2: 1,921.8 |
| 12 | q27 A-on | S1: 2,032.1; S2: 1,858.8 |

Execution-only session means:

| Query / case | Session 1 (ms) | Session 2 (ms) |
| --- | ---: | ---: |
| q80 Control-on | 838.5 | 839.2 |
| q80 A-on | 950.7 | 1,012.2 |
| q27 Control-on | 1,292.7 | 1,234.2 |
| q27 A-on | 1,503.6 | 1,375.3 |

## Plan Counters

Counters average all measured plans. Durations below are summed work,
not additive contributions to wall time. Bytes use decimal GB/MB.

| Workers | Query / case | Scan rows (M) | Read GB | Network MB | Predicate ms | Apply ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 12 | q80 Control-on | 55.47 | 2.142 | 1,353.2 | 1.9 | 0.0 |
| 12 | q80 A-on | 6.14 | 1.627 | 128.1 | 420.2 | 5.6 |
| 12 | q27 Control-on | 86.79 | 4.563 | 1,519.6 | 41.0 | 0.0 |
| 12 | q27 A-on | 5.70 | 4.572 | 140.4 | 4,726.3 | 367.9 |

## Scan CPU

Times are aggregate milliseconds across tasks and files. Decode and row
predicate CPU are nested within scan-poll CPU; do not add them together.

| Query / case | Prepare CPU | Reader build CPU | Scan poll CPU | Decode CPU | Row filter CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| q80 Control-on | 16.9 | 19.8 | 5,041.2 | 3,747.0 | 1.9 |
| q80 A-on | 140.5 | 40.0 | 3,500.4 | 2,613.0 | 419.9 |
| q27 Control-on | 19.7 | 23.1 | 12,184.5 | 9,366.9 | 40.9 |
| q27 A-on | 121.0 | 44.7 | 13,872.7 | 11,413.3 | 4,725.4 |

## Decoder Data Requests

Logical range-read calls, not HTTP requests. Summed await time includes
overlapping I/O and scheduling waits. Metadata is excluded.

| Query / case | Calls | Ranges | Bytes (GB) | Await ms | Poll CPU ms | Calls before first batch |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| q80 Control-on | 376 | 1,017 | 2.142 | 18,106.5 | 1,155.4 | 376 |
| q80 A-on | 632 | 12,083 | 1.623 | 24,073.9 | 794.4 | 632 |
| q27 Control-on | 474 | 2,204 | 4.563 | 38,150.0 | 2,503.4 | 474 |
| q27 A-on | 890 | 32,447 | 4.563 | 54,203.8 | 2,215.6 | 890 |

## Repartition Coalescing

EOF means the last input sender finished. Delay is first input to first
coalesced output, averaged over nonempty output buckets.

| Query / case | Regular rows (M) | EOF rows (M) | Rows at EOF | Buckets first emitting at EOF | First emit delay (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| q80 Control-on | 28.94 | 26.46 | 47.8% | 62.6% | 94.3 |
| q80 A-on | 0.00 | 6.14 | 100.0% | 100.0% | 38.4 |
| q27 Control-on | 70.32 | 16.23 | 18.8% | 29.4% | 175.7 |
| q27 A-on | 0.00 | 5.44 | 100.0% | 100.0% | 112.7 |

## Store Sales Stage Timing

First output is a task mean; finish is the slowest task. Timestamps are
relative to distributed query start. Coalescer delay starts at first input,
not query start. These aggregates must not be added as a latency breakdown.

| Query / case | Stage | First output | Finish | First emitting at EOF | Coalescer delay |
| --- | --- | ---: | ---: | ---: | ---: |
| q80 Control-on | Stage 6 | 581.4 | 770.7 | 0.0% | 141.3 |
| q80 A-on | Stage 6 | 800.9 | 906.4 | 100.0% | 82.7 |
| q27 Control-on | Stage 5 | 853.9 | 1,172.5 | 0.0% | 233.7 |
| q27 Control-on | Stage 11 | 894.7 | 1,182.5 | 0.0% | 236.5 |
| q27 Control-on | Stage 17 | 866.5 | 1,166.0 | 0.0% | 235.1 |
| q27 A-on | Stage 5 | 1,177.4 | 1,312.5 | 100.0% | 147.8 |
| q27 A-on | Stage 11 | 1,194.5 | 1,311.5 | 100.0% | 147.2 |
| q27 A-on | Stage 17 | 1,144.4 | 1,238.0 | 100.0% | 142.3 |

See [summary.json](summary.json) for per-table and per-stage metrics,
representative task timings, phase means, and every paired latency.
