import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(fileURLToPath(import.meta.url));
const metadata = JSON.parse(fs.readFileSync(path.join(root, 'metadata.json')));
const samples = JSON.parse(fs.readFileSync(path.join(root, 'samples.json')));
const names = ['all_off', 'dynamic_only', 'all_on'];
const mean = xs => xs.reduce((a, b) => a + b, 0) / xs.length;
const metric = (sample, name, predicate = () => true) => sample.metrics
  .filter(predicate).reduce((n, node) => n + (node.values[name] ?? 0), 0);
const scans = node => node.node === 'DistributedLeafExec' || node.node === 'DataSourceExec';
const groups = [];
for (const query of metadata.queries) {
  for (const name of names) {
    const rows = samples.filter(s => s.query === query && s.case.name === name && s.iteration > 0);
    if (rows.length !== metadata.measured_iterations) {
      throw new Error(`${query} ${name}: expected ${metadata.measured_iterations} samples, got ${rows.length}`);
    }
    const sorted = [...rows].sort((a, b) => a.execution_ms - b.execution_ms);
    const representative = sorted[Math.floor((sorted.length - 1) / 2)];
    const plan = `plans/${query}-${name}.txt`;
    fs.copyFileSync(path.join(root, representative.plan), path.join(root, plan));
    groups.push({
      query, case: name, n: rows.length,
      mean_ms: mean(rows.map(s => s.execution_ms)),
      median_ms: (sorted[Math.floor((sorted.length - 1) / 2)].execution_ms + sorted[Math.floor(sorted.length / 2)].execution_ms) / 2,
      min_ms: sorted[0].execution_ms,
      max_ms: sorted.at(-1).execution_ms,
      planning_mean_ms: mean(rows.map(s => s.planning_ms)),
      scan_rows: mean(rows.map(s => metric(s, 'output_rows', scans))),
      bytes_scanned: mean(rows.map(s => metric(s, 'bytes_scanned', scans))),
      pushdown_rows_pruned: mean(rows.map(s => metric(s, 'pushdown_rows_pruned', scans))),
      dynamic_row_groups_pruned: mean(rows.map(s => metric(s, 'row_groups_pruned_dynamic_filter', scans))),
      updates_received: mean(rows.map(s => metric(s, 'dynamic_filter_updates_received', n => n.node === 'DistributedExec'))),
      pushdown_eval_ms: mean(rows.map(s => metric(s, 'row_pushdown_eval_time', scans))) / 1e6,
      result_rows: rows[0].rows.length,
      representative_iteration: representative.iteration,
      plan,
    });
  }
}
fs.writeFileSync(path.join(root, 'summary.json'), JSON.stringify(groups, null, 2) + '\n');
const f = n => n.toFixed(1);
const lines = [
  '# TPC-H SF10 Local Dynamic-Filtering Matrix',
  '',
  `Source: \`${metadata.branch}\`, commit \`${metadata.commit}\`.`,
  '',
  fs.readFileSync(path.join(root, 'findings.md'), 'utf8').trim(),
  '',
  '## Setup',
  '',
  '- Release build; four separate localhost gRPC workers.',
  '- Four Tokio threads and four target partitions per worker.',
  '- Separate coordinator with two Tokio threads; all processes share 16 ARM cores.',
  '- One unmeasured warm-up per case; ten measured iterations per case.',
  '- Configuration order rotates and reverses between iterations.',
  '- Static task planning, broadcast joins enabled, LZ4 transport compression.',
  '- Distributed metrics and completed dynamic-filter collection enabled in every case.',
  '- Ordinary Parquet statistics pruning remains enabled in every case.',
  '- Execution time measures collecting the query output, including execution-time',
  '  coordination/reporting. SQL/physical planning, view setup/teardown, plan',
  '  formatting, result comparison, and artifact writes are outside this timer.',
  '- Warm-cache benchmark: no OS cache eviction between runs.',
  '- Every result is checked against the all-off warm-up for that query, with',
  '  rows sorted and exact per-cell string comparison.',
  '',
  `Data: \`${metadata.data}\`.`,
  '',
  `Generated SF10 locally, 16 files per scalable table, ${(metadata.tables.reduce((n,t)=>n+t.bytes,0)/2**30).toFixed(2)} GiB total.`,
  'No local SF100 Parquet dataset was found or generated. Existing remote',
  'benchmark metadata/results and correctness fixtures were preserved.',
  '',
  '## Configurations',
  '',
  '| Case | Dynamic filtering | Parquet pushdown | Filter reordering |',
  '| --- | --- | --- | --- |',
  '| all_off | false | false | false |',
  '| dynamic_only | true | false | false |',
  '| all_on | true | true | true |',
  '',
  'Keys:',
  '',
  '- `datafusion.optimizer.enable_dynamic_filter_pushdown`',
  '- `datafusion.execution.parquet.pushdown_filters`',
  '- `datafusion.execution.parquet.reorder_filters`',
  '',
  '## Latencies',
  '',
  'Milliseconds, ten measured runs. Speedup is all-off mean / case mean;',
  'values below 1 mean a slowdown. Each plan link is an executed plan with',
  'per-task metrics and reported runtime dynamic filters from a median-near run.',
  '',
  '| Query | Case | Mean | Median | Min | Max | Speedup | Plan |',
  '| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |',
];
for (const g of groups) {
  const base = groups.find(x => x.query === g.query && x.case === 'all_off');
  lines.push(`| ${g.query} | ${g.case} | ${f(g.mean_ms)} | ${f(g.median_ms)} | ${f(g.min_ms)} | ${f(g.max_ms)} | ${(base.mean_ms/g.mean_ms).toFixed(2)}x | [plan](${g.plan}) |`);
}
lines.push('', '## Scan and Filter Metrics', '',
  'Means over the same ten measured iterations. Scan totals sum distributed leaf',
  'scan metrics, including repeated scans of a table. Pushdown-pruned rows include both',
  'static and dynamic predicates; they are not solely remote-filter savings.',
  'Dynamic updates received are coordinator counts, not counts of rows saved.',
  '',
  '| Query | Case | Updates | Dynamic RG pruned | Scan output rows | Pushdown rows pruned | MiB scanned |',
  '| --- | --- | ---: | ---: | ---: | ---: | ---: |');
for (const g of groups) {
  lines.push(`| ${g.query} | ${g.case} | ${f(g.updates_received)} | ${f(g.dynamic_row_groups_pruned)} | ${f(g.scan_rows)} | ${f(g.pushdown_rows_pruned)} | ${f(g.bytes_scanned/2**20)} |`);
}
lines.push('', '## Interpretation Limits', '',
  '- Dynamic filtering toggles both local and remote filtering, not remote alone.',
  '- The all-on comparison also changes static-predicate row pushdown and ordering.',
  '- Timings are local shared-host observations, not remote-cluster predictions.',
  '- A final filter in a plan does not prove it arrived early enough to save work.',
  '- This four-query subset does not establish a whole-suite speedup.',
  '', '## Artifacts and Reproduction', '',
  '- [Raw timings, results, and per-node metrics](samples.json)',
  '- [Machine-readable summary](summary.json)',
  '- [Source, machine, and dataset metadata](metadata.json)',
  '- [Runner output](run.log)',
  '- [Runner source used for this run](tpch-filter-matrix.rs)',
  '- `plans/` retains every warm-up and measured iteration, plus 12 representative plans.',
  '', '```bash',
  'cd /home/bits/datafusion-distributed',
  'cargo build -p datafusion-distributed-benchmarks ' + String.fromCharCode(92),
  '  --bin tpch-filter-matrix --release --locked',
  'target/release/tpch-filter-matrix ' + String.fromCharCode(92),
  `  --data ${metadata.data} ` + String.fromCharCode(92),
  '  --output /path/to/new-results --iterations 10',
  '```', '');
fs.writeFileSync(path.join(root, 'report.md'), lines.join('\n'));
console.table(groups.map(g => ({query:g.query, case:g.case, mean_ms:f(g.mean_ms), median_ms:f(g.median_ms), updates:f(g.updates_received), dynamic_rg:f(g.dynamic_row_groups_pruned), scanned_MiB:f(g.bytes_scanned/2**20)})));
