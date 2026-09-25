# SF10: Local Versus Remote

Status: complete.

This table holds the locally selected A/B and Parquet mode fixed. A gain
below 1.0 means enabling dynamic filtering made that configuration slower.
It does not mean every dynamic-filter configuration for the query lost.
See [the remote matrix](matrix.md) for alternatives and
[the local findings](../local-dynamic-filter-sweep/findings.md) for selection.

| Query | Case | Local off ms | Local on ms | Gain | Remote off ms | Remote on ms | Gain |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q21 | B-off | 591.8 | 136.4 | 4.34x | 930.1 | 458.8 | 2.03x |
| Q37 | A-on | 801.3 | 211.9 | 3.78x | 1,720.7 | 717.5 | 2.40x |
| Q80 | B-on | 1,796.1 | 637.7 | 2.82x | 1,307.4 | 1,561.4 | 0.84x |
| Q80 | A-on | 1,742.0 | 640.1 | 2.72x | 1,307.4 | 1,377.4 | 0.95x |
| Q39 | B-off | 1,257.9 | 481.5 | 2.61x | 1,840.3 | 837.7 | 2.20x |
| Q27 | A-on | 2,437.6 | 1,012.0 | 2.41x | 2,179.6 | 2,393.2 | 0.91x |
| Q25 | A-on | 1,364.3 | 575.6 | 2.37x | 1,520.1 | 1,722.1 | 0.88x |
| Q82 | A-on | 852.6 | 377.5 | 2.26x | 1,675.3 | 867.2 | 1.93x |
| Q17 | B-on | 1,165.6 | 569.1 | 2.05x | 1,157.6 | 1,623.3 | 0.71x |
| Q26 | B-on | 617.4 | 305.8 | 2.02x | 1,931.6 | 1,278.5 | 1.51x |
| Q98 | B-on | 465.2 | 236.2 | 1.97x | 803.0 | 978.0 | 0.82x |

## What Changed

| Aspect | Local confirmation | Remote SF10 |
| --- | --- | --- |
| Hardware | One 16-core ARM Neoverse-N1 host | 12 c5n.4xlarge nodes |
| Workers | Four gRPC processes on localhost | One pod per node, 15 CPU / 38 GiB each |
| Target partitions | 4 per worker | 15 per worker |
| Maximum tasks per stage | 4 | Default uncapped setting (0) |
| Coordinator | Separate local process, two runtime threads | One of the measured worker pods |
| Input reads | Local instance storage, repeated warm-cache runs | S3 over the network |
| Timed work | Execution and result draining | Physical planning, execution, metrics, rendering |
| Measurements per case | 20, paired within two fresh-worker sessions | 10, in two five-run blocks |
| Disabled controls | Same binary for selected A and B cases | A controls bracket both builds |
| Correctness check | Compared result values, with documented tolerances | Compared result row counts only |
| Final predicate display | Completed filter snapshots applied | Endpoint omits snapshot rewrite |

Data is not a layout confounder here: all 384 files and
12,932,037,028 bytes match the recorded S3 ETags, including multipart
ETags. See [data-comparison.json](data-comparison.json). SQL and the frozen
A/B library source are the same; executable architecture and benchmark
adapters differ. Neither experiment injects post-scan FilterExecs.

Both use static planning, broadcast joins, union child isolation, LZ4
transport, and normal Parquet statistics pruning. These are not a
single-variable hardware or storage experiment: we cannot attribute a
wall-time difference precisely to S3, CPU, fanout, or reporting overhead.

Local execution-only milliseconds are not directly comparable to the
remote end-to-end endpoint milliseconds. Compare enabled versus disabled
inside each environment, then inspect whether work reduction persists.

The existing remote workers are larger than the tool's current defaults
(c5n.2xlarge, 7 CPU / 17 GiB). No worker shape was changed during timing.

## Work Reduction By Query

Counters below are means. Local counters come from numeric exports;
remote counters are approximate because the endpoint rounds text metrics.
Times sum operator measurements, not wall time. Network bytes include
transfer framing/compression and should not be read as unique source bytes.

The remote display can say `DynamicFilter [ empty ]` despite actual
pruning. The saved plans are unchanged; row and byte counters supply the
evidence. This display limitation cannot explain a timing improvement.

### Q21 B-off

Plans: [local disabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q21-B-off-2/plans/q21-Control-off-08-s0.txt),
[local enabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q21-B-off-2/plans/q21-B-off-06-s0.txt),
[remote disabled](plans/tpcds-sf10-q21-Control-off.txt),
[remote enabled](plans/tpcds-sf10-q21-B-off.txt).

| Metric | Local off | Local on | Remote off | Remote on |
| --- | ---: | ---: | ---: | ---: |
| Latency ms | 591.8 | 136.4 | 930.1 | 458.8 |
| Tasks | 11.0 | 11.0 | 7.0 | 7.0 |
| Scan output rows | 133,285,059.0 | 4,975,059.0 | 133,295,060.0 | 4,975,060.0 |
| Scan bytes | 463,079,082.0 | 26,242,119.0 | 463,110,000.0 | 26,162,000.0 |
| Row-pushdown rejects | 0.0 | 0.0 | 0.0 | 0.0 |
| Predicate evaluation ms | 0.0 | 0.0 | 0.0 | 0.0 |
| Join input rows | 274,634,550.0 | 9,929,731.0 | 274,633,062.0 | 9,916,662.0 |
| Join compute ms | 4,668.5 | 173.1 | 3,932.6 | 136.4 |
| Network bytes | 2,062,887.8 | 2,062,651.3 | 2,334,289.9 | 2,383,390.7 |
| Remote updates | 0.0 | 0.0 | 0.0 | 0.0 |
| Global-hash merges | 0.0 | 0.0 | - | 0.0 |
| Spilled bytes | 0.0 | 0.0 | 0.0 | 0.0 |

### Q37 A-on

Plans: [local disabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q37-A-on-2/plans/q37-Control-on-02-s0.txt),
[local enabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q37-A-on-2/plans/q37-A-on-09-s0.txt),
[remote disabled](plans/tpcds-sf10-q37-Control-on.txt),
[remote enabled](plans/tpcds-sf10-q37-A-on.txt).

| Metric | Local off | Local on | Remote off | Remote on |
| --- | ---: | ---: | ---: | ---: |
| Latency ms | 801.3 | 211.9 | 1,720.7 | 717.5 |
| Tasks | 18.0 | 18.0 | 40.0 | 40.0 |
| Scan output rows | 65,062,899.0 | 4,047,654.6 | 65,060,866.0 | 676.0 |
| Scan bytes | 505,935,371.0 | 70,206,439.6 | 506,855,800.0 | 70,411,800.0 |
| Row-pushdown rejects | 82,623,411.0 | 15,328,655.4 | 82,624,990.0 | 19,375,090.0 |
| Predicate evaluation ms | 151.1 | 69.8 | 291.2 | 101.6 |
| Join input rows | 52,466,615.0 | 507,387.7 | 50,829,026.0 | 964.0 |
| Join compute ms | 197.7 | 7.2 | 241.3 | 52.8 |
| Network bytes | 20,254,929.5 | 5,640,175.6 | 7,815,864.3 | 721,449.0 |
| Remote updates | 0.0 | 10.8 | 0.0 | 31.4 |
| Global-hash merges | 0.0 | 0.0 | - | - |
| Spilled bytes | 0.0 | 0.0 | 0.0 | 0.0 |

### Q80 B-on

Plans: [local disabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q80-B-on-2/plans/q80-Control-on-04-s0.txt),
[local enabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q80-B-on-2/plans/q80-B-on-02-s0.txt),
[remote disabled](plans/tpcds-sf10-q80-Control-on.txt),
[remote enabled](plans/tpcds-sf10-q80-B-on.txt).

| Metric | Local off | Local on | Remote off | Remote on |
| --- | ---: | ---: | ---: | ---: |
| Latency ms | 1,796.1 | 637.7 | 1,307.4 | 1,561.4 |
| Tasks | 55.0 | 55.0 | 106.0 | 106.0 |
| Scan output rows | 55,465,002.0 | 6,205,877.5 | 55,465,783.0 | 6,138,973.0 |
| Scan bytes | 2,142,328,368.0 | 1,633,958,744.2 | 2,142,175,850.0 | 1,627,269,850.0 |
| Row-pushdown rejects | 508,005.0 | 36,177,884.3 | 508,014.0 | 36,065,524.0 |
| Predicate evaluation ms | 1.2 | 490.2 | 1.6 | 414.3 |
| Join input rows | 108,191,137.0 | 9,641,733.0 | 108,407,676.0 | 9,725,396.0 |
| Join compute ms | 4,228.2 | 623.5 | 5,154.4 | 1,452.8 |
| Network bytes | 1,022,040,815.6 | 87,745,594.7 | 1,351,802,316.8 | 128,143,022.1 |
| Remote updates | 0.0 | 79.4 | 0.0 | 214.0 |
| Global-hash merges | 0.0 | 0.0 | - | 0.0 |
| Spilled bytes | 0.0 | 0.0 | 0.0 | 0.0 |

### Q80 A-on

Plans: [local disabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q80-A-on-2/plans/q80-Control-on-07-s0.txt),
[local enabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q80-A-on-2/plans/q80-A-on-10-s0.txt),
[remote disabled](plans/tpcds-sf10-q80-Control-on.txt),
[remote enabled](plans/tpcds-sf10-q80-A-on.txt).

| Metric | Local off | Local on | Remote off | Remote on |
| --- | ---: | ---: | ---: | ---: |
| Latency ms | 1,742.0 | 640.1 | 1,307.4 | 1,377.4 |
| Tasks | 55.0 | 55.0 | 106.0 | 106.0 |
| Scan output rows | 55,465,002.0 | 6,225,107.6 | 55,465,783.0 | 6,138,973.0 |
| Scan bytes | 2,142,328,368.0 | 1,627,475,771.8 | 2,142,175,850.0 | 1,627,210,850.0 |
| Row-pushdown rejects | 508,005.0 | 35,986,638.5 | 508,014.0 | 36,065,524.0 |
| Predicate evaluation ms | 1.2 | 483.5 | 1.6 | 430.0 |
| Join input rows | 108,191,137.0 | 9,680,193.6 | 108,407,676.0 | 9,725,396.0 |
| Join compute ms | 4,122.1 | 608.8 | 5,154.4 | 1,378.4 |
| Network bytes | 1,022,006,005.5 | 88,120,691.7 | 1,351,802,316.8 | 127,533,127.7 |
| Remote updates | 0.0 | 80.3 | 0.0 | 214.6 |
| Global-hash merges | 0.0 | 0.0 | - | - |
| Spilled bytes | 0.0 | 0.0 | 0.0 | 0.0 |

### Q39 B-off

Plans: [local disabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q39-B-off-2/plans/q39-Control-off-05-s0.txt),
[local enabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q39-B-off-2/plans/q39-B-off-02-s0.txt),
[remote disabled](plans/tpcds-sf10-q39-Control-off.txt),
[remote enabled](plans/tpcds-sf10-q39-B-off.txt).

| Metric | Local off | Local on | Remote off | Remote on |
| --- | ---: | ---: | ---: | ---: |
| Latency ms | 1,257.9 | 481.5 | 1,840.3 | 837.7 |
| Tasks | 26.0 | 26.0 | 16.0 | 16.0 |
| Scan output rows | 266,570,118.0 | 4,966,118.0 | 266,590,120.0 | 4,970,120.0 |
| Scan bytes | 919,701,650.0 | 32,758,046.0 | 919,793,200.0 | 32,707,200.0 |
| Row-pushdown rejects | 0.0 | 0.0 | 0.0 | 0.0 |
| Predicate evaluation ms | 0.0 | 0.0 | 0.0 | 0.0 |
| Join input rows | 799,547,633.0 | 14,735,633.0 | 799,199,468.0 | 14,339,468.0 |
| Join compute ms | 7,879.8 | 222.8 | 8,289.2 | 220.6 |
| Network bytes | 49,099,151.9 | 48,578,642.7 | 58,716,600.3 | 57,683,087.4 |
| Remote updates | 0.0 | 0.0 | 0.0 | 0.0 |
| Global-hash merges | 0.0 | 0.0 | - | 0.0 |
| Spilled bytes | 0.0 | 0.0 | 0.0 | 0.0 |

### Q27 A-on

Plans: [local disabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q27-A-on-2/plans/q27-Control-on-07-s0.txt),
[local enabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q27-A-on-1/plans/q27-A-on-04-s0.txt),
[remote disabled](plans/tpcds-sf10-q27-Control-on.txt),
[remote enabled](plans/tpcds-sf10-q27-A-on.txt).

| Metric | Local off | Local on | Remote off | Remote on |
| --- | ---: | ---: | ---: | ---: |
| Latency ms | 2,437.6 | 1,012.0 | 2,179.6 | 2,393.2 |
| Tasks | 40.0 | 40.0 | 96.0 | 96.0 |
| Scan output rows | 86,792,481.0 | 5,762,224.7 | 86,789,508.0 | 5,701,608.0 |
| Scan bytes | 4,563,408,858.0 | 4,572,574,212.0 | 4,563,062,310.0 | 4,571,953,310.0 |
| Row-pushdown rejects | 5,898,306.0 | 86,928,562.3 | 5,888,214.0 | 87,008,214.0 |
| Predicate evaluation ms | 25.2 | 1,178.2 | 40.0 | 4,850.7 |
| Join input rows | 89,195,481.0 | 6,908,790.3 | 91,650,006.0 | 9,305,886.0 |
| Join compute ms | 1,024.2 | 103.3 | 958.9 | 391.9 |
| Network bytes | 1,213,035,731.8 | 80,807,589.3 | 1,521,408,051.2 | 140,396,001.3 |
| Remote updates | 0.0 | 87.9 | 0.0 | 262.7 |
| Global-hash merges | 0.0 | 0.0 | - | - |
| Spilled bytes | 0.0 | 0.0 | 0.0 | 0.0 |

### Q25 A-on

Plans: [local disabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q25-A-on-2/plans/q25-Control-on-02-s0.txt),
[local enabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q25-A-on-2/plans/q25-A-on-06-s0.txt),
[remote disabled](plans/tpcds-sf10-q25-Control-on.txt),
[remote enabled](plans/tpcds-sf10-q25-A-on.txt).

| Metric | Local off | Local on | Remote off | Remote on |
| --- | ---: | ---: | ---: | ---: |
| Latency ms | 1,364.3 | 575.6 | 1,520.1 | 1,722.1 |
| Tasks | 29.0 | 29.0 | 68.0 | 68.0 |
| Scan output rows | 46,180,244.0 | 4,861,295.4 | 46,180,560.0 | 2,108,690.0 |
| Scan bytes | 1,028,223,755.0 | 856,019,343.7 | 1,028,200,240.0 | 788,652,240.0 |
| Row-pushdown rejects | 218,689.0 | 32,336,731.2 | 218,680.0 | 31,541,174.0 |
| Predicate evaluation ms | 0.3 | 522.9 | 0.8 | 1,338.8 |
| Join input rows | 48,581,811.0 | 5,187,925.3 | 49,400,082.0 | 3,255,925.0 |
| Join compute ms | 3,656.8 | 502.6 | 4,677.2 | 1,143.7 |
| Network bytes | 739,567,062.6 | 71,808,156.5 | 1,052,491,038.7 | 120,869,570.6 |
| Remote updates | 0.0 | 45.1 | 0.0 | 139.9 |
| Global-hash merges | 0.0 | 0.0 | - | - |
| Spilled bytes | 0.0 | 0.0 | 0.0 | 0.0 |

### Q82 A-on

Plans: [local disabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q82-A-on-1/plans/q82-Control-on-08-s0.txt),
[local enabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q82-A-on-1/plans/q82-A-on-03-s0.txt),
[remote disabled](plans/tpcds-sf10-q82-Control-on.txt),
[remote enabled](plans/tpcds-sf10-q82-A-on.txt).

| Metric | Local off | Local on | Remote off | Remote on |
| --- | ---: | ---: | ---: | ---: |
| Latency ms | 852.6 | 377.5 | 1,675.3 | 867.2 |
| Tasks | 18.0 | 18.0 | 40.0 | 40.0 |
| Scan output rows | 79,462,639.0 | 1,357,679.5 | 79,460,076.0 | 2,811.0 |
| Scan bytes | 554,409,280.0 | 122,514,304.0 | 555,845,800.0 | 122,665,800.0 |
| Row-pushdown rejects | 82,623,401.0 | 32,554,360.5 | 82,624,990.0 | 33,914,990.0 |
| Predicate evaluation ms | 143.8 | 1,771.3 | 283.0 | 1,294.6 |
| Join input rows | 57,878,431.0 | 342,870.1 | 51,646,352.0 | 3,577.0 |
| Join compute ms | 213.3 | 9.6 | 268.7 | 61.7 |
| Network bytes | 62,201,396.2 | 2,920,811.7 | 35,721,451.5 | 837,724.2 |
| Remote updates | 0.0 | 10.9 | 0.0 | 31.6 |
| Global-hash merges | 0.0 | 0.0 | - | - |
| Spilled bytes | 0.0 | 0.0 | 0.0 | 0.0 |

### Q17 B-on

Plans: [local disabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q17-B-on-2/plans/q17-Control-on-07-s0.txt),
[local enabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q17-B-on-2/plans/q17-B-on-04-s0.txt),
[remote disabled](plans/tpcds-sf10-q17-Control-on.txt),
[remote enabled](plans/tpcds-sf10-q17-B-on.txt).

| Metric | Local off | Local on | Remote off | Remote on |
| --- | ---: | ---: | ---: | ---: |
| Latency ms | 1,165.6 | 569.1 | 1,157.6 | 1,623.3 |
| Tasks | 29.0 | 29.0 | 68.0 | 68.0 |
| Scan output rows | 46,180,425.0 | 5,352,067.9 | 46,180,741.0 | 2,866,991.0 |
| Scan bytes | 352,808,531.0 | 318,040,038.7 | 352,751,140.0 | 306,390,140.0 |
| Row-pushdown rejects | 218,508.0 | 31,451,895.7 | 218,520.0 | 31,023,060.0 |
| Predicate evaluation ms | 1.4 | 635.4 | 2.9 | 1,141.6 |
| Join input rows | 48,582,551.0 | 5,718,279.1 | 49,402,270.0 | 4,054,922.0 |
| Join compute ms | 3,302.3 | 453.8 | 3,774.6 | 812.5 |
| Network bytes | 639,754,636.9 | 71,236,216.9 | 933,631,150.1 | 134,175,385.6 |
| Remote updates | 0.0 | 47.3 | 0.0 | 137.7 |
| Global-hash merges | 0.0 | 2.0 | - | 2.0 |
| Spilled bytes | 0.0 | 0.0 | 0.0 | 0.0 |

### Q26 B-on

Plans: [local disabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q26-B-on-1/plans/q26-Control-on-08-s0.txt),
[local enabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q26-B-on-1/plans/q26-B-on-01-s0.txt),
[remote disabled](plans/tpcds-sf10-q26-Control-on.txt),
[remote enabled](plans/tpcds-sf10-q26-B-on.txt).

| Metric | Local off | Local on | Remote off | Remote on |
| --- | ---: | ---: | ---: | ---: |
| Latency ms | 617.4 | 305.8 | 1,931.6 | 1,278.5 |
| Tasks | 16.0 | 16.0 | 40.0 | 40.0 |
| Scan output rows | 14,531,565.0 | 2,990,065.8 | 14,531,104.0 | 1,853,004.0 |
| Scan bytes | 812,783,415.0 | 232,244,742.8 | 812,722,510.0 | 221,024,510.0 |
| Row-pushdown rejects | 1,966,045.0 | 2,551,310.8 | 1,962,682.0 | 3,439,852.0 |
| Predicate evaluation ms | 7.3 | 30.8 | 11.7 | 782.1 |
| Join input rows | 15,123,639.0 | 3,418,483.8 | 15,955,318.0 | 3,104,548.0 |
| Join compute ms | 176.1 | 44.2 | 223.2 | 105.0 |
| Network bytes | 219,717,322.2 | 47,015,378.7 | 294,135,920.6 | 56,970,936.3 |
| Remote updates | 0.0 | 34.1 | 0.0 | 103.3 |
| Global-hash merges | 0.0 | 1.0 | - | 1.0 |
| Spilled bytes | 0.0 | 0.0 | 0.0 | 0.0 |

### Q98 B-on

Plans: [local disabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q98-B-on-1/plans/q98-Control-on-07-s0.txt),
[local enabled](../local-dynamic-filter-sweep/runs/confirm-tpcds-q98-B-on-2/plans/q98-B-on-06-s0.txt),
[remote disabled](plans/tpcds-sf10-q98-Control-on.txt),
[remote enabled](plans/tpcds-sf10-q98-B-on.txt).

| Metric | Local off | Local on | Remote off | Remote on |
| --- | ---: | ---: | ---: | ---: |
| Latency ms | 465.2 | 236.2 | 803.0 | 978.0 |
| Tasks | 14.0 | 14.0 | 38.0 | 38.0 |
| Scan output rows | 28,831,458.0 | 109,296.0 | 28,830,471.0 | 109,291.0 |
| Scan bytes | 555,389,805.0 | 558,444,923.0 | 555,345,800.0 | 558,331,800.0 |
| Row-pushdown rejects | 144,582.0 | 28,866,744.0 | 144,580.0 | 28,884,580.0 |
| Predicate evaluation ms | 1.8 | 199.4 | 1.9 | 172.4 |
| Join input rows | 37,512,188.0 | 279,526.0 | 37,744,552.0 | 523,292.0 |
| Join compute ms | 2,001.6 | 23.7 | 987.7 | 40.6 |
| Network bytes | 15,131,956.5 | 15,120,830.7 | 47,368,458.2 | 47,353,753.6 |
| Remote updates | 0.0 | 0.0 | 0.0 | 0.0 |
| Global-hash merges | 0.0 | 0.0 | - | 0.0 |
| Spilled bytes | 0.0 | 0.0 | 0.0 | 0.0 |
