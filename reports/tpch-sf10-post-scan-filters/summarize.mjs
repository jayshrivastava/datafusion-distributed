import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(fileURLToPath(import.meta.url));
const baseline = path.join(root, '../tpch-sf10-local-filter-matrix');
const samples = JSON.parse(fs.readFileSync(path.join(root, 'samples.json')));
const previous = JSON.parse(fs.readFileSync(path.join(baseline, 'summary.json')));
const previousSamples = JSON.parse(fs.readFileSync(path.join(baseline, 'samples.json')));
const queries = ['q17', 'q18', 'q20', 'q15'];
const cases = ['all_off', 'dynamic_only', 'all_on'];
const mean = xs => xs.reduce((a, b) => a + b, 0) / xs.length;
const sumMetric = (sample, key, predicate = () => true) => sample.metrics
  .filter(predicate).reduce((sum, node) => sum + (node.values[key] ?? 0), 0);
const scans = node => ['DataSourceExec', 'DistributedLeafExec'].includes(node.node);
const postScan = node => node.post_scan_filter && node.dynamic_filter;
const summary = [];

if (samples.length !== 132) throw Error(`Expected 132 executions, got ${samples.length}`);
for (const query of queries) {
  const expected = JSON.stringify(previousSamples.find(s => s.query === query).rows);
  for (const sample of samples.filter(s => s.query === query)) {
    if (JSON.stringify(sample.rows) !== expected) throw Error(`${query}: result mismatch`);
    if (!fs.readFileSync(path.join(root, sample.plan), 'utf8').includes('metrics=[')) {
      throw Error(`${sample.plan}: no metrics`);
    }
  }
  for (const name of cases) {
    const rows = samples.filter(s => s.query === query && s.case.name === name && s.iteration > 0);
    if (rows.length !== 10) throw Error(`${query} ${name}: expected 10 measured iterations`);
    const sorted = rows.toSorted((a, b) => a.execution_ms - b.execution_ms);
    const representative = sorted[4];
    const plan = `plans/${query}-${name}.txt`;
    fs.copyFileSync(path.join(root, representative.plan), path.join(root, plan));
    const metric = (key, predicate) => mean(rows.map(s => sumMetric(s, key, predicate)));
    const scanRows = metric('output_rows', scans);
    const input = metric('input_rows', postScan);
    const output = metric('output_rows', postScan);
    summary.push({
      query, case: name, n: rows.length,
      mean_ms: mean(rows.map(s => s.execution_ms)),
      median_ms: (sorted[4].execution_ms + sorted[5].execution_ms) / 2,
      min_ms: sorted[0].execution_ms,
      max_ms: sorted.at(-1).execution_ms,
      scan_rows: scanRows,
      post_scan_input_rows: input,
      post_scan_output_rows: output,
      post_scan_rows_removed: input - output,
      forwarded_scan_rows: scanRows - input + output,
      post_scan_compute_ms: metric('elapsed_compute', postScan) / 1e6,
      shuffle_bytes: metric('bytes_transferred', n => n.node === 'NetworkShuffleExec'),
      bytes_scanned: metric('bytes_scanned', scans),
      dynamic_row_groups_pruned: metric('row_groups_pruned_dynamic_filter', scans),
      updates_received: metric('dynamic_filter_updates_received'),
      pushdown_rows_pruned: metric('pushdown_rows_pruned', scans),
      pushdown_eval_ms: metric('row_pushdown_eval_time', scans) / 1e6,
      result_rows: rows[0].rows.length,
      representative_iteration: representative.iteration,
      plan,
    });
  }
}
fs.writeFileSync(path.join(root, 'summary.json'), JSON.stringify(summary, null, 2) + '\n');
const f = n => n.toFixed(1);
const millions = n => (n / 1e6).toFixed(3);
const lines = [
  '# TPC-H SF10: Post-Scan Dynamic Filters', '',
  'Uncommitted experiment on `js/6-apply-merged-filters`.', '',
  '## Setup', '',
  '- Release build; four localhost worker processes.',
  '- Four worker threads and four target partitions per worker.',
  '- One warm-up and ten measured executions per matrix entry.',
  '- Static task planning; existing SF10 Parquet data; warm filesystem cache.',
  '- Metrics and dynamic-filter reporting enabled in every case.',
  '- All 132 results match the previous all-off results exactly.',
  '- Planning and plan rendering are outside the execution timer.', '',
  '| Case | Dynamic filters | Parquet pushdown | Reordering |',
  '| --- | --- | --- | --- |',
  '| all_off | false | false | false |',
  '| dynamic_only | true | false | false |',
  '| all_on | true | true | true |', '',
  'Ordinary Parquet statistics pruning stays enabled in all cases.',
  'The post-scan fallback is active only in `dynamic_only`.', '',
  '## Latencies', '',
  'Milliseconds over ten runs. Speedup is the new all-off mean divided by',
  'the case mean. A value below 1 means slower.', '',
  '| Query | Case | Mean | Median | Min | Max | Speedup |',
  '| --- | --- | ---: | ---: | ---: | ---: | ---: |',
];
for (const g of summary) {
  const off = summary.find(x => x.query === g.query && x.case === 'all_off');
  lines.push(`| ${g.query} | ${g.case} | ${f(g.mean_ms)} | ${f(g.median_ms)} | ${f(g.min_ms)} | ${f(g.max_ms)} | ${(off.mean_ms/g.mean_ms).toFixed(2)}x |`);
}
lines.push('', '## Compared With the Previous Run', '',
  'Historical means, not an interleaved before/after experiment. The all-off',
  'and all-on execution paths were not intentionally changed; their movement',
  'provides context for host and scheduling variability.', '',
  '| Query | Case | Previous ms | New ms | Previous / new |',
  '| --- | --- | ---: | ---: | ---: |');
for (const g of summary) {
  const old = previous.find(x => x.query === g.query && x.case === g.case);
  lines.push(`| ${g.query} | ${g.case} | ${f(old.mean_ms)} | ${f(g.mean_ms)} | ${(old.mean_ms/g.mean_ms).toFixed(2)}x |`);
}
lines.push('', '## Rows After Scans', '',
  'Mean row counts in millions, summed across scans, including repeated table',
  'scans. Forwarded rows subtract the new immediate post-scan filter drops.',
  'Other downstream filters are not subtracted.', '',
  '| Query | Case | Scan output M | Post-scan removed M | Forwarded M |',
  '| --- | --- | ---: | ---: | ---: |');
for (const g of summary) {
  lines.push(`| ${g.query} | ${g.case} | ${millions(g.scan_rows)} | ${millions(g.post_scan_rows_removed)} | ${millions(g.forwarded_scan_rows)} |`);
}
lines.push('', '## Filter Costs and Pruning', '',
  'Compute milliseconds sum across tasks, not wall-clock query latency.',
  'Parquet pushdown counts include both static and dynamic predicates.', '',
  '| Query | Case | Post-scan compute ms | Pushdown eval ms | Dynamic RG pruned |',
  '| --- | --- | ---: | ---: | ---: |');
for (const g of summary) {
  lines.push(`| ${g.query} | ${g.case} | ${f(g.post_scan_compute_ms)} | ${f(g.pushdown_eval_ms)} | ${f(g.dynamic_row_groups_pruned)} |`);
}
lines.push('', '## Shuffle Traffic', '',
  'MiB transferred by NetworkShuffleExec, averaged over ten runs.', '',
  '| Query | All off | Dynamic only | All on |',
  '| --- | ---: | ---: | ---: |');
for (const q of queries) {
  const values = cases.map(name => summary.find(g => g.query === q && g.case === name));
  lines.push(`| ${q} | ${values.map(g => f(g.shuffle_bytes / 2**20)).join(' | ')} |`);
}
if (fs.existsSync(path.join(root, 'findings.md'))) {
  lines.push('', fs.readFileSync(path.join(root, 'findings.md'), 'utf8').trim());
}
lines.push('', '## Executed Plans', '',
  'Representative plans are from a median-near measured execution. Every',
  'warm-up and measured plan is also retained in `plans/`.', '',
  '| Query | All off | Dynamic only | All on |',
  '| --- | --- | --- | --- |');
for (const q of queries) {
  lines.push(`| ${q} | [plan](plans/${q}-all_off.txt) | [plan](plans/${q}-dynamic_only.txt) | [plan](plans/${q}-all_on.txt) |`);
}
lines.push('', '## Artifacts', '',
  '- [Raw timings, result rows, and operator metrics](samples.json)',
  '- [Machine-readable summary](summary.json)',
  '- [Implementation scope](README.md)',
  '- [Previous report](../tpch-sf10-local-filter-matrix/report.md)',
  '- [Runner log](run.log)',
  '- [Metadata](metadata.json)', '',
  'These four queries do not establish a whole-suite performance improvement.',
  'The all-on case also changes static row filtering, so its benefit cannot',
  'be attributed solely to remote dynamic filters.', '');
fs.writeFileSync(path.join(root, 'report.md'), lines.join('\n'));
console.table(summary.map(g => ({
  query: g.query, case: g.case, ms: f(g.mean_ms),
  removed_M: millions(g.post_scan_rows_removed),
  forwarded_M: millions(g.forwarded_scan_rows),
})));
