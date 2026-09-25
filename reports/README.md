# Dynamic Filtering Reports

Newest first:

- [Confirmed local dynamic-filter wins](local-dynamic-filter-sweep/findings.md)
- [Exhaustive local dynamic-filter matrix](local-dynamic-filter-sweep/matrix.md)
- [Q18 SF10: repeated dynamic-filter on/off comparison](tpch-sf10-q18-repeatability/findings.md)
- [Remote SF100: global-hash filter matrix](remote-global-hash-filters/findings.md)
- [Global hash CASE with task offsets](tpch-sf10-global-hash-filters/report.md)
- [Post-scan dynamic filters](tpch-sf10-post-scan-filters/report.md)
- [Original SF10 filter matrix](tpch-sf10-local-filter-matrix/report.md)

Each directory includes the reports, executed plans with metrics, raw samples,
and supporting analysis files. Relative links between reports are preserved.

The older experiments' original artifacts remain under
`/home/bits/datafusion-distributed-dev-tools/.data/`.
Their compiled runner executables were not duplicated here. The Q18
repeatability directory includes its own saved release runner and provenance.
