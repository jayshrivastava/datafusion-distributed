# Dynamic-Filter Routes

These classifications come from the executed remote plans, not from the
local benchmark's worker layout. `Local` means producer and consumer share
a stage/task; `remote` means the relationship crosses a stage boundary.
A network boundary on a join's build side does not itself make the join's
probe-side filter remote.

The frozen endpoint does not rewrite collected predicate snapshots for
display, so consumers show `DynamicFilter [ empty ]`. Consumer placement
still identifies the routes below. This is not evidence that every listed
filter performed useful pruning. Use the measured row/byte counters for
that, and do not attribute join savings to a TopK merely present in a plan.
None of these queries has an ungrouped MIN/MAX filter producer.

## SF10

Reviewed against the first A-on pass. `C` is a CollectLeft hash join; `P`
is a partitioned hash join. Stage numbers refer to the linked plans.

| Query | Remote Producers | Local Producers |
| --- | --- | --- |
| [Q21][q21] | None | C joins |
| [Q37][q37] | P join, TopK | C joins |
| [Q80][q80] | C joins | None |
| [Q39][q39] | None | C joins |
| [Q27][q27] | C/P joins, TopK | None |
| [Q25][q25] | C/P joins | None |
| [Q82][q82] | P join, TopK | C joins |
| [Q17][q17] | C/P joins | None |
| [Q26][q26] | C/P joins, TopK | None |
| [Q98][q98] | None | C joins |

- Q21: stage 4's date/item/warehouse joins filter its inventory scan.
  The coordinator reports zero remote updates.
- Q37: stage 5's partitioned semi-join filters catalog sales in stage 4.
  Stage 3 has local inventory filters. Stage 6's TopK has an item consumer
  in stage 2.
- Q80: CollectLeft joins in stages 7, 14, and 21 filter the sales scans in
  stages 6, 13, and 20. The partitioned Right joins below those producers
  are not the source of these probe-side filters.
- Q39: the two inventory branches have local joins/consumers in stages
  4 and 9. No remote updates are reported.
- Q27: joins in stages 6, 12, and 18 filter the three store-sales scans in
  stages 5, 11, and 17. Stage 19's TopK reaches item scans in stages 1 and 7.
- Q25: stage 10's date/dimension filters reach catalog sales, store
  returns, and store sales in stages 6, 7, and 8. Its partitioned join
  filters store returns; stage 9's partitioned join filters store sales.
- Q82: the Q37 pattern with store sales in stage 4 instead of catalog
  sales. Local inventory filters remain in stage 3; the item TopK consumer
  is in stage 2.
- Q17: the same stage-level filter routes as Q25, with different SQL
  predicates and projections. Every first-pass SF10 case returns zero rows.
- Q26: stage 6's joins filter catalog sales in stage 5. Stage 7's TopK
  has an item consumer in stage 2.
- Q98: stage 3's date/item joins filter store sales in the same stage.
  No remote updates are reported.

## SF100

Reviewed against the first A-on pass as it completes. Unlisted queries
are still awaiting inspection; do not infer their routes from SF10.

| Query | Remote Producers | Local Producers |
| --- | --- | --- |
| [Q21][q21-100] | None | C joins |
| [Q37][q37-100] | P join, TopK | C joins |
| [Q80][q80-100] | C joins | None |
| [Q39][q39-100] | C/P joins | None |
| [Q27][q27-100] | C/P joins, TopK | None |
| [Q25][q25-100] | C/P joins | None |
| [Q82][q82-100] | P join, TopK | C joins |

- Q21 keeps the SF10 stage topology. Its inventory scan and three local
  joins execute together in stage 4, now with six tasks. Remote updates
  remain zero.
- Q37 also retains the same routes. Stage 3's six inventory tasks have
  local join consumers. The stage 5 semi-join filters catalog sales in
  stage 4, and stage 6's TopK reaches the item scan in stage 2.
- Q80 retains its remote CollectLeft producers in stages 7, 14, and 21,
  with consumers in stages 6, 13, and 20. Its partitioned Right joins
  are not the producers of those four sales-side filter relationships.
- Q39 changes from local to remote scan consumers. Each item join is
  now partitioned: joins in stages 5 and 10 filter the inventory scans
  in stages 4 and 9. The CollectLeft date and warehouse filters also
  cross those shuffles. These are remote-filter gains, unlike SF10's
  otherwise similar inventory-pruning gains.
- Q27 adds an extra shuffle for its now-partitioned item joins. Item
  producers in stages 7, 14, and 21 reach store-sales consumers in
  stages 5, 12, and 19, through join stages 6, 13, and 20. Those middle
  stages also produce date/store/demographic filters for the scans.
  Stage 22's TopK reaches item scans in stages 1 and 8.
- Q25 adds a partitioned item join in stage 11. Its item filter reaches
  store sales in stage 8, through stage 10. The other routes retain the
  SF10 numbering: stage 10's filters reach sales/returns in stages 6,
  7, and 8; stage 9's join also filters store sales in stage 8.
- Q82 retains the Q37 topology, with store sales in stage 4 replacing
  catalog sales. Stage 3's inventory filters remain local, while the
  stage 5 semi-join and stage 6 TopK have remote consumers.

[q21]: plans/tpcds-sf10-q21-A-on.txt
[q37]: plans/tpcds-sf10-q37-A-on.txt
[q80]: plans/tpcds-sf10-q80-A-on.txt
[q39]: plans/tpcds-sf10-q39-A-on.txt
[q27]: plans/tpcds-sf10-q27-A-on.txt
[q25]: plans/tpcds-sf10-q25-A-on.txt
[q82]: plans/tpcds-sf10-q82-A-on.txt
[q17]: plans/tpcds-sf10-q17-A-on.txt
[q26]: plans/tpcds-sf10-q26-A-on.txt
[q98]: plans/tpcds-sf10-q98-A-on.txt
[q21-100]: plans/tpcds-sf100-q21-A-on.txt
[q37-100]: plans/tpcds-sf100-q37-A-on.txt
[q80-100]: plans/tpcds-sf100-q80-A-on.txt
[q39-100]: plans/tpcds-sf100-q39-A-on.txt
[q27-100]: plans/tpcds-sf100-q27-A-on.txt
[q25-100]: plans/tpcds-sf100-q25-A-on.txt
[q82-100]: plans/tpcds-sf100-q82-A-on.txt
