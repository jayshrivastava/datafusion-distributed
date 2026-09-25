# A Repeatable Local Win: TPC-H Q18 SF10

**Turning dynamic filtering on makes Q18 1.45x faster on this local setup.**
Mean execution time falls from 2024 ms to 1398 ms, a 31% reduction.
The same B/global-hash binary is used on both sides. Parquet row pushdown
and reordering are enabled in both cases; only dynamic filtering changes.

This is not a comparison between two filtering implementations, nor an
all-off versus all-on comparison that also changes Parquet settings.

## Repeatability

| Fresh worker session | DF off ms | DF on ms | Speedup | Faster pairs |
| --- | ---: | ---: | ---: | ---: |
| 1 | 2021 | 1430 | 1.41x | 19/20 |
| 2 | 2036 | 1323 | 1.54x | 20/20 |
| 3 | 2015 | 1442 | 1.40x | 17/20 |
| Combined | 2024 | 1398 | 1.45x | 56/60 |

- Sixty measured executions per case, plus warmups, with balanced off/on
  ordering. Workers restart between sessions. No samples were removed.
- The paired bootstrap 95% interval for the mean speedup is 1.38-1.51x.
  All criteria written in [the plan](plan.md) before timing passed.
- All 126 executions return identical values for all 624 result rows,
  matching both the disabled runs and the earlier saved SF10 reference.

The improvement is repeatable in the average, not guaranteed on every run.
Four enabled executions were slower; they remain in every reported mean.

## Why It Helps

Q18 first finds orders whose total lineitem quantity exceeds 300. There are
624 qualifying order keys in this dataset. The joins turn their build-side
keys into dynamic predicates and send updates to remote consumers through
the coordinator. The existing SQL and predicate threshold are unchanged.
The exact [repository Q18 SQL](round-1/q18.sql), without a LIMIT, is saved.

```text
lineitem -> GROUP BY order key -> HAVING SUM(quantity) > 300
                                          |
                              qualifying join build keys
                                          |
                           coordinator merges remote updates
                                /                    \
                               v                      v
                       orders scan             second lineitem scan
                       fewer rows                 fewer rows
                                \                    /
                                 joins and shuffles
                                          |
                                  624 result rows
```

Means across all 60 measured executions per case:

| Work | DF off | DF on |
| --- | ---: | ---: |
| Stage 1 initial lineitem scan output | 59.986 M | 59.986 M |
| Stage 2 customer scan output | 1.500 M | 1.500 M |
| Stage 3 orders scan output | 15.000 M | 1.410 M |
| Stage 5 second lineitem scan output | 59.986 M | 26.252 M |
| All network output rows | 106.487 M | 45.572 M |
| Network transfer | 836.9 MB | 240.8 MB |
| Join compute, summed seconds | 13.07 | 4.78 |
| Parquet predicate evaluation, summed seconds | 0.00 | 4.44 |

- Row pushdown rejects an average of 47.325 million rows before they enter
  the distributed joins and shuffles. The scan predicates contain actual
  hash-routed IN-list filters, not merely a nonzero update counter.
- Network bytes fall 71% and join compute falls 63%. Here those savings
  outweigh the added predicate evaluation cost. The initial aggregation
  scan remains unfiltered, as expected: it has to discover the qualifying keys.
- Dynamic row-group pruning is zero and bytes scanned stay at 1227.2 MB.
  This is a row-filtering and downstream-work win, not skipped Parquet I/O.

Inspect the [disabled plan][off] and [enabled plan][on], especially Stage 3
and Stage 5. They are executed plans with per-task metrics and reported
runtime predicates. The tables above average raw numeric metrics from all
runs; these two linked plans are representative median-near executions.

The enabled representative emits only 624 rows from orders, compared with
15 million disabled. Its second lineitem scan emits 17.829 million rows,
compared with 59.986 million disabled. Both execution plans return 624 rows.

There are three successful global-hash merges in every enabled execution
and an average of 20.15 remote updates. Every coordinator-worker channel
in the representative plan is remote: these are separate localhost worker
processes, not shared in-memory task execution.

## What The Slow Runs Show

In the four slower enabled executions, the second lineitem scan forwards
59.46-59.99 million rows, almost its full input. Orders forwards 11.83-15.00
million rows. They still pay predicate costs while avoiding little work.
That is consistent with useful updates arriving too late; these counters
do not directly timestamp update arrival.

Examples: [round 1, pair 1][slow1] and [round 3, pair 11][slow2]. The latter
forwards every orders and lineitem row despite eventually receiving filter
updates. This explains why an update counter alone is insufficient evidence.

## Scope And SF100

The result is specific to B, local SF10, warm files, and four workers with
four partitions each on this 16-core ARM machine. The post-scan FilterExec
experiment is inactive because Parquet row pushdown is enabled.

There is no claim that A has the same benefit or that this is a suite-wide
improvement. No ClickBench or TPC-DS search was needed to obtain this result.

SF100 portability remains unproven. The previous twelve-worker remote
SF100 test was slower with dynamic filtering, despite reducing rows. It
uses up to 180 hash branches instead of 16, different hardware, S3, and
a timer including planning and plan rendering. The local win should not
be presented as an SF100 win without a separate controlled experiment.

For reproduction commands, latency distributions, and all measurements,
see [report.md](report.md). The release binary, source provenance, SQL,
raw samples, and all executed plans are saved in this directory.

[off]: round-3/plans/q18-dynamic_off-11.txt
[on]: round-2/plans/q18-dynamic_on-18.txt
[slow1]: round-1/plans/q18-dynamic_on-01.txt
[slow2]: round-3/plans/q18-dynamic_on-11.txt
