# Confirmation Selection

Selected after completing all 326 screening process blocks, before starting
confirmation. There are 19 enabled configurations across 17 distinct queries.

- Take the 15 highest matched-control screening gains among distinct queries
  with matching results and observed work reduction. Keep TPC-DS Q17 despite
  its empty result, and Q82 despite its marginal best-disabled comparison;
  report those limitations rather than changing the selection after reruns.
- Also confirm TPC-H Q7 and ClickBench Q26, the strongest remaining
  metric-backed candidates in those suites.
- Repeat TPC-DS Q21 and Q80 with A. Their selected B plans report no global
  hash merges, so these repeats check whether the existing implementation
  reproduces the local-only and remote-filter examples respectively.

Each configuration gets two fresh-worker sessions, ten measured repetitions
plus one warmup per case per session, and both disabled controls. Reverse the
initial case order for the second session. B candidates use B controls.

Do not qualify the 136 screening result-set differences across ClickBench
Q17, Q24, Q31, Q32, Q38, Q39, Q40, and Q41 as performance wins. Differences
also occur within disabled-control repeats. Q17 has no ORDER BY; the others
do not specify a total order for LIMIT/OFFSET. This is consistent with
nondeterministic selection, not evidence by itself of a dynamic-filter bug.
No tie-aware equivalence has been established for those samples.

TPC-DS Q72's disabled controls timed out during warmup. Its enabled runs
cannot be compared with a completed control result and are not shortlisted.

All discarded or unqualified results remain in the full matrix and raw
artifacts. Screening is not the final ranking.
