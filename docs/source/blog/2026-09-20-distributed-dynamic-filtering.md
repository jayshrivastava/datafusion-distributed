---
layout: post
title: "Optimizing Distributed Joins, Sorts, and Aggregates with Dynamic Filtering"
date: 2026-09-20
author: "Jayant Shrivastava (@jayshrivastava)"
categories: [features]
---

# Optimizing Distributed Joins, Sorts, and Aggregates with Dynamic Filtering

*September 20, 2026 · Jayant Shrivastava ([@jayshrivastava])*

```{contents}
:local:
:depth: 2
```

## Background and Motivation

DataFusion implements an optimization called [dynamic filtering](https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/)
which applies filters discovered during execution, but its shared-memory
mechanism **does not automatically work** when producers and consumers run in
a distributed environment.

Let's take a look at dynamic filtering in a single process. Consider this query, which joins a small `dim` table
with a large `fact` table:

```sql
SELECT f.*
FROM fact f
JOIN (
    SELECT d_key
    FROM dim
    WHERE region = 'EMEA'
) d
ON f.d_key = d.d_key;
```

A hash join first reads the small input, called the **build side**, and creates a
hash table from its join keys. It then reads the large **probe side** and looks
up each probe key in that table.

Without dynamic filtering, the probe side of the join incurs overhead for rows that will be discarded by
the join anyways:
- reading and decoding files
- materialing columnar buffers
- hashing join columns
- any hypothetical operators that may exist between the data source and the join
  - evaluating expressions
  - aggregations
  - etc.

With dynamic filtering, the join's filter can be pushed down to the data source, reducing
the rows early.

```{figure} ../_static/images/dynamic-filtering/single-node-dynamic-filter.svg
:alt: Worker A sends build keys B and D to a hash join on Worker B. The colocated hash join sends an in-memory filter update to its fact-table scan and receives only matching rows B and D.
:width: 100%

**Figure 1**
```

Figure 1 shows a distributed plan where Worker A sends build rows to the hash join on Worker B. The hash join
producer and probe-side data source consumer are colocated on Worker B, so the
join can propagate a runtime filter to the scan using  **shared-memory**.

However, remote plan nodes do not share memory, so a consumer on
another worker cannot see the update directly.

```{figure} ../_static/images/dynamic-filtering/remote-probe-cannot-share-filter.svg
:alt: The hash join runs on Worker A, the build scan on Worker B, and the probe scan on Worker C. The join's shared-memory update stops at the process boundary, so the remote probe emits every row.
:width: 100%

**Figure 2**
```

Avoiding this work is valuable in single-machine scenario. In a distributed query, it can be
even more valuable because pruning rows at the scan-level helps avoid other types of overhead:
- serialization
- network transfer
- shuffle size

In this post, we will discuss how we implemented Dynamic Filtering in DataFusion-Distributed
to support non-colocated (a.k.a. remote) dynamic filters and analyze performance
using TPC-DS queries.


## Design

The design has two requirements.

1. Allow Dynamic Filters to Cross Network Boundaries
2. Route Dynamic Filters from Producers to the Corresponding Consumers

Note that (2) is tricky because dynamic filter producers such as joins may be partitioned across multiple machines
and see different data, producing distinct filters. Furthermore, consumers may be partitioned differently from
the producers.

To meet these requirements, we use a similar approach to [Trino](https://trino.io/docs/current/admin/dynamic-filtering.html)
and Spark:

1. Discover Dynamic Filter Producers and Consumers
2. Collect Dynamic Filters from Producers
3. Merge Dynamic Filters
4. Broadcast the Merged Filters to Consumers

### 1. Discover Dynamic Filter Producers and Consumers

Consider the plan below, divided vertically into 4 stages and horizonally into separate workers/tasks,
with Stage 1 running in 4 workers, Stage 2 running in 4 tasks etc.

We traverse the plan an annotate where the producers and consumers are. In Stage 3, there's 2 `HashJoinExec`
nodes producing dynamic filters in 2 tasks. In Stage 2, there's 4 `HashJoinExec` nodes producing
dynamic filters in 4 tasks. Finally, in Stage 1, there's a consumer `DataSourceExec` utilizing both filters.

In the sections below, we will discuss how filters are collected and safetly merged.

```text

┌───── DistributedExec
│ CoalescePartitionsExec
│   [Stage 3] => NetworkCoalesceExec
└──────────────────────────────────────────────
  ┌───── Stage 3 ── tasks=2
  │ DistributedExec
  │ AggregateExec
  │   HashJoinExec producers=[1]
  │     DataSourceExec
  │     AggregateExec
  │       [Stage 2] NetworkShuffleExec
  └──────────────────────────────────────────────
    ┌───── Stage 2 ── tasks=4
    │ RepartitionExec
    │   HashJoinExec producers=[2]
    │     DataSourceExec
    │     AggregateExec
    │       NetworkShuffleExec
    └──────────────────────────────────────────────
      ┌───── Stage 1  ── tasks=8
      │ RepartitionExec
      │   AggregateExec
      │     DataSourceExec consumers=[1, 2]
      └──────────────────────────────────────────────
```


### 2. Collect and Merge Dynamic Filters from Producers

#### Partitioned Hash Join

```{figure} ../_static/images/dynamic-filtering/remote-partitioned-join.svg
:alt: Two Stage 1 build tasks send separate dimension partitions to two hash joins. After the joins build their hash tables and report filters, four Stage 2 probe tasks receive the merged filter and scan the fact table.
:width: 100%

Figure 3
```

The join executes in two tasks, each producing a different filter, `F0 = key in (B, D)` and
`F1 = key in (G, H)`.

At the consumers, a particular row does not necessarily know which producer it will route to,
so we have to take the conservative approach of waiting for all producer filters to be reported
and unioning them before passing them on: `Fglobal = F0 OR F1 OR ... OR Fn`. This merged filter
is applied to each row at the scan level.

#### A Note on `CASE hash(expr)`

Every task is actually partitioned into multiple partitions denoted by [`target_partitions`](https://datafusion.apache.org/user-guide/configs.html),
often by hash partitioning. For partitioned joins, each join actually produces per-partition filters and
produces a `CASE hash(row) % num_partitions` expression which can be applied to each row.

Assume `target_partitions=4` for this running example.

```text
CASE hash(row) % 4
  WHEN 0 THEN F0_P0(row)
  WHEN 1 THEN F0_P1(row)
  WHEN 2 THEN F0_P2(row)
  WHEN 3 THEN F0_P3(row)
END
```

In Figure 3, we have 2 tasks making a total of 8 global partitions across the tasks but two
filters with 4 partitions each:

```text
CASE hash(row) % 4
  WHEN 0 THEN F0_P0(row)
  WHEN 1 THEN F0_P1(row)
  WHEN 2 THEN F0_P2(row)
  WHEN 3 THEN F0_P3(row)
END

OR

CASE hash(row) % 4
  WHEN 0 THEN F1_P0(row)
  WHEN 1 THEN F1_P1(row)
  WHEN 2 THEN F1_P2(row)
  WHEN 3 THEN F1_P3(row)
END
```

The question is, is this correct? The answer is **yes** due to this property:

```text
(hash(key) % M) % N = hash(key) % N, when M is a multiple of N
```

`M` in this example would be `2 tasks * 4 target_partitions = 8` and `N` would be `target_partitions=4`.
Say for example a row is routed to global partition `5` (ie. partition 1 in producer task 1). In other words,
`hash(row) % 8 = 5`. By the property, `hash(row) % 4` must be 1. Therefore, the
correct filter `F1_P1` is applied to the row.

Note that `F0_P1` would also be applied. This is safe because, if `F0_P1` rejected the row, `F1_P1`
can choose to admit it because of the `OR`. The only downside is some loss of selectivity
(we effectively have 4 filters instead of 8) in favor of simplicity. A more selective alternative
is one large expression keyed by the global partition index across all the tasks:

```text
CASE hash(row) % 8
  WHEN 0 THEN F0_P0(row)
  ...
  WHEN 4 THEN F1_P0(row)
  ...
  WHEN 7 THEN F1_P3(row)
END
```

This increases selectivity and evalutes at most one predicate per row. Because this
design depends modifying the actual expression, it can be considered brittle
if such expressions change upstream in DataFusion core. It can also create a
large `CASE` expression that is expensive for a data source to evaluate. Since
the cost of expression evaluation is one of the expensive tradeoffs when
enabling dynamic filters, this can have a significant impact.

Because predicate evaluation and optimization is [ongoing work](https://github.com/apache/datafusion/issues/22883) upstream,
today's approach favoring the less brittle approach of just `OR`ing the filters.

#### `CollectLeft` Hash Join

Every task receives the same complete build side. Their predicates are
equivalent, so the coordinator can just forward the first completed filter
to all the consumers.

```{figure} ../_static/images/dynamic-filtering/remote-collect-left-join.svg
:alt: One Stage 1 build task scans the dimension table and broadcasts its complete build side to two CollectLeft joins. The coordinator accepts the first equivalent filter, then four Stage 2 probe tasks apply it to the fact table.
:width: 100%

Figure 4
```

#### MIN/MAX Aggregate

Partial aggreates which compute `MIN` or `MAX` on a column with no group by
push down filters to prune rows outside the current `MIN` or `MAX` bound.

Each update is independently safe to push down as long as we `AND` updates
from different producer tasks.

As shown in Figure 5, we track the global `MIN` of the query and continously
push that minumum value down to the scan tasks. 

```{figure} ../_static/images/dynamic-filtering/remote-min-aggregate.svg
:alt: Two partial MIN aggregates send their local minima through a final aggregate while reporting successively lower bounds, which the coordinator intersects with AND and sends to remote scans.
:width: 100%

Figure 5
```

#### TopK Sort

A distributed TopK sort operation is compsed of a partitioned, distributed sort
where each `SortExec` retains its best `K` candidates. This is followed by a sort preserving merge
to get the global TopK values.

When the sort and scan are colocated, each sort pushes down a dynamic filter to
eliminate rows that cannot enter its per-task top `K`. With remote filters, we
select the best `K` to push down. In Figure 6, the query computes the top 3 values.
Each `SortExec` passes its bound to the coorindator and the coorindator chooses the
tighest bound to push down.

```{figure} ../_static/images/dynamic-filtering/remote-topk-sort.svg
:alt: Two per-task TopK sorts send sorted candidates through a SortPreservingMerge while reporting increasingly strict bounds, which the coordinator intersects with AND and sends to remote scans.
:width: 100%

Figure 6
```

### 3. Broadcasting Merged Filters to Consumers

Once a merged predicate is ready to be sent, the coordinator sends the filter
to each worker containing a matching consumer. Each worker applies the filter
to its physical plan during execution.

Consumers do not necessarily wait for remote filters. With sorts and aggregates,
the scan often starts before the filter arrives. With joins, the probe side
waits to be polled (ie. waits for the build side).

## Contributing Upstream

Implementing distributed dynamic filtering exposed several opportunities to contribute
useful changes to the [apache/datafusion](https://github.com/apache/datafusion) core itself.

`ExecutionPlan::apply_expressions()` ([#24018](https://github.com/apache/datafusion/pull/24018)) restored a general API for
visiting physical expressions owned by physical execution plan nodes. Distributed
DataFusion uses it to find dynamic-filter consumers. It also adds a way for custom
`ExecutionPlan` implementations can opt in to dynamic filtering by exposing their expressions
through the same interface.

`ExecutionPlan::dynamic_expressions_produced()` ([#24068](https://github.com/apache/datafusion/pull/24068)) added the
complementary producer-facing API to `apply_expressions`. Visiting a plan's
expressions is enough to find consumers, but distributed routing must also know
the direction to send updates. The new trait method exposes a way to find producers
explicitly, so users can discover producers without hardcoding the
`HashJoinExec`, `SortExec`, and `AggregateExec` operators.

[Serialize and deduplicate dynamic filters (#21807)] taught DataFusion's
protobuf conversion to preserve shared dynamic-filter identity. If a producer
and consumer reference the same memory before serialization, they must
continue to share the same memory decoded expression afterwards.

[Serialize dynamic filters on sort, aggregate, and hash-join plans (#22011)]
Ensures operators encode their dynamic filters rather than dropping them on serialization.

Together these changes make dynamic expressions in DataFusion discoverable, serializable,
and identity-preserving. Distributed DataFusion adds network routing on top,
but the underlying plans and expressions follow standard DataFusion practices.

## Benchmarks

We benchmarked dynamic filtering on TPC-DS at `scale_factor=10` in both a
local benchmark (in-memory workers on one machine) and a distributed benchmark
on multiple machines using the [distributed benchmark dev tools](https://github.com/gabotechs/datafusion-distributed-dev-tools/tree/main/benchmarks-remote).

### Local Benchmark Results

Figure 7 shows the ten largest improvements in TPC-DS SF10, run on a 16-core machine
on local data with 4 in-memory workers configured with `datafusion.execution.target_partitions=4`.

`parquet={on,off}` refers to the following DataFusion configuration options:
- `datafusion.execution.parquet.pushdown_filters`: evaluates predicates during Parquet decoding to avoid decoding rejected rows (“late materialization”)
- `datafusion.execution.parquet.reorder_filters`: heuristically reorders pushed-down predicates to reduce evaluation cost

The average speedup across the full TPC-DS suite was `1.05x` with
`parquet=off` and `1.20x` with `parquet=on`.

This does not mean `parquet=on` enables file-level pruning: statistics and
page-index pruning remain enabled in both configurations. Instead, it lets the
Parquet reader apply the dynamic predicate while decoding rows. The larger gain
shows that dynamic filters become more useful when the scan can act on them at
more levels—files and row groups through statistics, and rows through late
materialization. The [upstream DataFusion dynamic-filtering results](https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/)
showed the same compounding effect: ClickBench Q23 improved by up to **22x**
when dynamic filters and late materialization were enabled together.

```{figure} ../_static/images/dynamic-filtering/local-tpcds-speedup.svg
:alt: Grouped bar chart comparing local TPC-DS query speedups with dynamic filtering off and on, each with Parquet row-filter pushdown and filter reordering off and on.
:width: 100%

**Figure 7**
```

[Q80](https://github.com/datafusion-contrib/datafusion-distributed/blob/a43ea4703f9d1ef668abd8f5e6865e05814ed549/testdata/tpcds/queries/q80.sql?plain=1#L3)
demonstrates remote dynamic filter propagation in action; every dynamic filter
in the query had to cross a network boundary. The query contains several joins
with small dimension tables and large fact tables like `store_sales`,
`catalog_sales`, and `web_sales`.

| Metric, mean across 20 runs | Dynamic filters off | Dynamic filters on |
|---|---:|---:|
| Speedup | 1.00x | **2.72x** |
| Scan Output Rows, Summed | 55.47 million | **6.23 million** |
| Join Input Rows, Summed | 108.19 million | **9.68 million** |
| Total Network Transfer | 1.02 GB | **88.1 MB** |
| Join Compute, Summed | 4,122 ms | **609 ms** |
| Coordinate Dynamic Filter Updates Recieved | 0 | **80.3** |

Latency improved **2.72x**. Network traffic fell by more than 90%,
showing why scan-level filtering matters in a distributed plan: the
rejected rows never entered the intervening joins or shuffles.

[Q37](https://github.com/datafusion-contrib/datafusion-distributed/blob/a43ea4703f9d1ef668abd8f5e6865e05814ed549/testdata/tpcds/queries/q37.sql?plain=1#L1)
combines colocated and remote dynamic filters. A partitioned LeftSemi join
sends an item-key filter across a shuffle to `catalog_sales`, while a colocated
date filter is applied to `inventory`.

| Metric (parquet=on) | Dynamic filters off | Dynamic filters on |
|---|---:|---:|
| Speedup | 1.00x | **3.78x** |
| Scan Output Rows, Summed | 65.06 million | **4.05 million** |
| Join Input Rows, Summed | 52.47 million | **507 thousand** |
| Total Network Transfer | 20.25 MB | **5.64 MB** |
| Join Compute, Summed | 198 ms | **7 ms** |
| Coordinator Dynamic Filter Updates Received | 0 | **10.8** |

A remote dynamic filter reduces `catalog_sales` from 14.40 million to 4.05
million rows, and a colocated date filter also reduces `inventory` from 50.66
million rows to 74, a **93.8% reduction**.

### Distributed Benchmark Results

Figure 8 shows the distributed results for the same 10 queries above. These were run on 12
`c5n.4xlarge` instances with 15 cores and `target_partitions` each using data on `S3`.

```{figure} ../_static/images/dynamic-filtering/remote-tpcds-speedup.svg
:alt: Grouped bar chart comparing distributed TPC-DS query speedups with dynamic filtering off and on, each with Parquet row-filter pushdown and filter reordering off and on.
:width: 100%

**Figure 8**
```

Only five queries reproduced a performance gain (in any `parquet` configuration).

We investigated Q80 with 20 instrumented runs per setting. Only dynamic
filtering changed. The filters arrived and worked, but pushing them into the
critical S3 scan changed both the read and shuffle pipelines.

[Q80](https://github.com/datafusion-contrib/datafusion-distributed/blob/a43ea4703f9d1ef668abd8f5e6865e05814ed549/testdata/tpcds/queries/q80.sql?plain=1#L3)
is the cleanest demonstration that this is an I/O and streaming effect, not
simply expensive predicate evaluation.

| Q80 metric, mean across 20 runs | Dynamic filters off | Dynamic filters on |
|---|---:|---:|
| Execution speedup | 1.00x | **0.855x** |
| First-result latency | 803 ms | **943 ms** |
| Critical `store_sales` output | 28.800 M rows | **0.592 M rows** |
| Decoder data requested | 1.058 GB | **1.058 GB** |
| Decoder reads per output stream | 1 | **5** |
| Data-read await time, summed | 4.782 s | **9.023 s** |
| Mean file-stream first-batch delay | 151 ms | **339 ms** |
| Critical fact-stage finish | 771 ms | **906 ms** |
| Total network transfer | 1.353 GB | **128 MB** |
| Critical scan-poll CPU, summed | 2.801 s | **2.432 s** |

Q80 removes 97.9% of critical scan rows and 91% of network traffic, yet execution
increases from 839 to 982 ms. Scan CPU falls 13%, ruling out expensive predicate
evaluation as the cause. Instead, the critical stage finishes 136 ms later,
matching the 140 ms increase in first-result latency. I/O savings on
`catalog_sales` occur on an earlier, non-critical branch.

```{figure} ../_static/images/dynamic-filtering/remote-scan-read-pipeline.svg
:alt: Four timelines compare dynamic filtering off and on in both the local and distributed benchmarks. Progressive reads are cheap against warm local storage, while every dependent S3 read adds I/O latency and sparse shuffle output waits for end-of-stream.
:width: 100%

**Figure 9**
```

Figure 9 compares filtering off and on in each environment. DataFusion's Parquet
data source changes from one combined read to a series of dependent reads when
it evaluates pushed-down filters. That I/O-pattern change is cheap against warm
local storage, so avoiding join and shuffle work produces a **2.72x** speedup.
Against S3, every dependent read adds I/O latency. The filter prunes no pages or
row groups, so the scan requests the same bytes, and sparse output also delays
shuffle batches until end-of-stream. The result is a **0.855x** speedup.

This regression is a byproduct of the current DataFusion Parquet data source,
not an inherent cost of remote dynamic filtering. Internal execution nodes or
file formats that apply a dynamic predicate without changing their read pattern
avoid this specific penalty. [DataFusion issue #24393](https://github.com/apache/datafusion/issues/24393)
tracks separating predicate evaluation from the choice between one-shot and
progressive reads. Summed await times overlap and are not additive wall-time;
the critical-stage finish and first-result latency show the end-to-end effect.

## Conclusion

Distributed dynamic filtering is implemented with DataFusion's native physical
plan and expression APIs. Producers expose dynamic expressions through
`ExecutionPlan`, stable expression IDs preserve their relationships across
serialization and stage boundaries, and consumers continue to use DataFusion's
existing scan pushdown and pruning machinery. Colocated filters retain the
shared-memory path, while the coordinator merges and forwards only the updates
that cross processes. The mechanism remains fail-open: it can eliminate work,
but it cannot change a query result.

Dynamic filtering is a tradeoff. Propagating and merging filters, preparing
file-specific predicates, and evaluating them at scans all cost CPU and
coordination. That cost pays off when a selective filter arrives early enough
to skip files, row groups, or pages, or prevents substantial join and network
work. It helps less when data has already been read, the predicate is expensive,
or many tasks produce repeated updates. The benchmark results reflect this:
some queries improve substantially, while others reduce rows and network
traffic without improving end-to-end latency.

Special thanks to Andrew Lamb ([@alamb]), Adrian Garcia Badaracco
([@adriangb]), Lía Adriana ([@LiaCastaneda]), Gabriel Musat Mestre
([@GabrielMusat]), Gene Bordegaray ([@gene-bordegaray]), and the
[Apache DataFusion community] for their design, implementation, and review work.

[Trino dynamic filtering]: https://trino.io/docs/current/admin/dynamic-filtering.html
[Spark runtime filtering]: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/read/SupportsRuntimeV2Filtering.html
[the original implementation by Lía Adriana (#20337)]: https://github.com/apache/datafusion/pull/20337
[Serialize and deduplicate dynamic filters (#21807)]: https://github.com/apache/datafusion/pull/21807
[Serialize dynamic filters on sort, aggregate, and hash-join plans (#22011)]: https://github.com/apache/datafusion/pull/22011
[@jayshrivastava]: https://github.com/jayshrivastava
[@alamb]: https://github.com/alamb
[@adriangb]: https://github.com/adriangb
[@LiaCastaneda]: https://github.com/LiaCastaneda
[@GabrielMusat]: https://github.com/GabrielMusat
[@gene-bordegaray]: https://github.com/gene-bordegaray
[Apache DataFusion community]: https://datafusion.apache.org/community/
