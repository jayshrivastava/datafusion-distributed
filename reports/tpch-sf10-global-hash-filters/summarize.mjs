import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(fileURLToPath(import.meta.url));
const queries = ['q17', 'q18', 'q20', 'q15'];
const cases = ['all_off', 'dynamic_only', 'all_on'];
const mean = xs => xs.reduce((sum, x) => sum + x, 0) / xs.length;
const metric = (s, name, accept = () => true) => s.metrics.filter(accept)
  .reduce((sum, m) => sum + (m.values[name] ?? 0), 0);
const scan = m => ['DataSourceExec', 'DistributedLeafExec'].includes(m.node);
const postScan = m => m.post_scan_filter && m.dynamic_filter;
const expected = new Map();
const summary = [];
for (const binary of ['baseline', 'candidate']) {
  const samples = [1, 2].flatMap(block => {
    const directory = `${binary}-${block}`;
    const rows = JSON.parse(fs.readFileSync(path.join(root, directory, 'samples.json')));
    if (rows.length !== 72) throw Error(`${directory}: expected 72 executions`);
    return rows.map(s => ({ ...s, directory }));
  });
  for (const s of samples) {
    const result = JSON.stringify(s.rows);
    if (!expected.has(s.query)) expected.set(s.query, result);
    if (result !== expected.get(s.query)) throw Error(`Results differ: ${s.query}`);
    const plan = fs.readFileSync(path.join(root, s.directory, s.plan), 'utf8');
    if (!plan.includes('metrics=[')) throw Error(`Missing plan metrics: ${s.plan}`);
  }
  for (const query of queries) for (const name of cases) {
    const rows = samples.filter(s => s.query === query && s.case.name === name && s.iteration > 0);
    if (rows.length !== 10) throw Error(`${binary} ${query} ${name}: expected ten samples`);
    const sorted = rows.toSorted((a, b) => a.execution_ms - b.execution_ms);
    const representative = sorted[4];
    const m = (key, predicate) => mean(rows.map(s => metric(s, key, predicate)));
    summary.push({
      binary, query, case: name, n: rows.length,
      mean_ms: mean(rows.map(s => s.execution_ms)),
      median_ms: (sorted[4].execution_ms + sorted[5].execution_ms) / 2,
      min_ms: sorted[0].execution_ms, max_ms: sorted.at(-1).execution_ms,
      block_means: [1, 2].map(i => mean(rows.filter(s => s.directory.endsWith(`-${i}`))
        .map(s => s.execution_ms))),
      scan_rows: m('output_rows', scan),
      forwarded_rows: m('output_rows', scan) - m('input_rows', postScan) + m('output_rows', postScan),
      post_scan_removed: m('input_rows', postScan) - m('output_rows', postScan),
      post_scan_compute_ms: m('elapsed_compute', postScan) / 1e6,
      pushdown_compute_ms: m('row_pushdown_eval_time', scan) / 1e6,
      pushdown_rows_pruned: m('pushdown_rows_pruned', scan),
      dynamic_row_groups_pruned: m('row_groups_pruned_dynamic_filter', scan),
      shuffle_bytes: m('bytes_transferred', m => m.node === 'NetworkShuffleExec'),
      bytes_scanned: m('bytes_scanned', scan),
      global_merges: m('dynamic_filter_global_hash_merges'),
      updates_received: m('dynamic_filter_updates_received'),
      result_rows: rows[0].rows.length,
      plan: `${representative.directory}/${representative.plan}`,
    });
  }
}
fs.writeFileSync(path.join(root, 'summary.json'), JSON.stringify(summary, null, 2) + '\n');
const routing = {};
for (const binary of ['baseline', 'candidate']) {
  const file = `${binary}-1/plans/q17-dynamic_only-01.txt`;
  const plan = fs.readFileSync(path.join(root, file), 'utf8');
  const filter = plan.split('\n').find(line => line.includes('FilterExec: task_variants='));
  const firstVariant = filter.split('t1:')[0];
  const predicates = [...firstVariant.matchAll(/CASE hash_repartition % (\d+) (.*?) END/g)]
    .map(match => ({
      modulo: Number(match[1]),
      branches: [...match[2].matchAll(/WHEN (\d+) THEN (.*?)(?= WHEN \d+ THEN| ELSE |$)/g)]
        .map(branch => ({ bucket: Number(branch[1]),
          predicate: branch[2].includes('IN (SET)') ? 'IN-list and bounds' : 'bounds only' })),
    }));
  routing[binary] = { file, predicates };
}
if (routing.candidate.predicates.length !== 1 ||
    routing.candidate.predicates[0].modulo !== 16 ||
    JSON.stringify(routing.candidate.predicates[0].branches.filter(b => b.predicate === 'bounds only')
      .map(b => b.bucket)) !== '[7]') throw Error('Q17 routing evidence changed; revise findings');
fs.writeFileSync(path.join(root, 'predicate-routing.json'), JSON.stringify(routing, null, 2) + '\n');
const get = (binary, query, name) => summary.find(s => s.binary === binary && s.query === query && s.case === name);
const f = x => x.toFixed(1);
const million = x => (x / 1e6).toFixed(3);
const lines = [
  '# TPC-H SF10: Global Hash Dynamic Filters', '',
  'Uncommitted experiment. See [setup and routing safeguards](README.md).', '',
  'The comparison isolates the coordinator merge change. Both binaries include',
  'the earlier post-scan filter experiment. Four localhost workers each use',
  'four target partitions and four worker threads.', '',
  'Ten measured executions per entry, in A/B/B/A blocks of five. Each block',
  'also has one warmup per entry. All 288 executions return identical results',
  'across configurations and binaries, and match the previous benchmark.', '',
  '## Conclusion', '',
  '- q17 benefits from both cheaper evaluation and more selective predicates:',
  '  about 1.7x faster than the old merge, with 44% fewer forwarded scan rows',
  '  in dynamic-only mode. It is still slower than filtering disabled.',
  '- q18 improves by 1.30x in dynamic-only mode and 1.36x with all options on.',
  '  All-on is also 1.54x faster than its own all-off control.',
  '- q20 regresses by 1.27x in dynamic-only mode and 1.05x with all options on.',
  '  Its predicates filter too little to repay their evaluation cost.',
  '- No dynamic row groups were pruned; scanned bytes were unchanged.',
  '- q15 has no partitioned join and performs no global merges.', '',
  '## Latency', '',
  'Means in milliseconds. Speedup is baseline / candidate, not all-off / on.', '',
  '| Query | Configuration | Baseline | Global hash | Speedup |',
  '| --- | --- | ---: | ---: | ---: |',
];
for (const q of queries) for (const c of cases) {
  const a = get('baseline', q, c), b = get('candidate', q, c);
  lines.push(`| ${q} | ${c} | ${f(a.mean_ms)} | ${f(b.mean_ms)} | ${(a.mean_ms/b.mean_ms).toFixed(2)}x |`);
}
lines.push('', '## Filtering', '',
  'Rows forwarded after scans and immediate post-scan filters, in millions.',
  'Includes repeated table scans; downstream filters are not subtracted.', '',
  '| Query | Configuration | Baseline M | Global hash M |',
  '| --- | --- | ---: | ---: |');
for (const q of queries) for (const c of ['dynamic_only', 'all_on']) {
  const a = get('baseline', q, c), b = get('candidate', q, c);
  lines.push(`| ${q} | ${c} | ${million(a.forwarded_rows)} | ${million(b.forwarded_rows)} |`);
}
lines.push('', '## Filter Compute', '',
  'Summed operator compute milliseconds across tasks, not query wall time.',
  'Dynamic-only uses post-scan compute; all-on uses Parquet pushdown evaluation.', '',
  '| Query | Configuration | Baseline ms | Global hash ms |',
  '| --- | --- | ---: | ---: |');
for (const q of queries) for (const c of ['dynamic_only', 'all_on']) {
  const a = get('baseline', q, c), b = get('candidate', q, c);
  const key = c === 'dynamic_only' ? 'post_scan_compute_ms' : 'pushdown_compute_ms';
  lines.push(`| ${q} | ${c} | ${f(a[key])} | ${f(b[key])} |`);
}
lines.push('', '## Routing and Row Groups', '',
  'Mean global merges and dynamically pruned row groups per query execution.', '',
  '| Query | Configuration | Global merges | Baseline RG | Global hash RG |',
  '| --- | --- | ---: | ---: | ---: |');
for (const q of queries) for (const c of ['dynamic_only', 'all_on']) {
  const a = get('baseline', q, c), b = get('candidate', q, c);
  lines.push(`| ${q} | ${c} | ${f(b.global_merges)} | ${f(a.dynamic_row_groups_pruned)} | ${f(b.dynamic_row_groups_pruned)} |`);
}
if (fs.existsSync(path.join(root, 'findings.md'))) {
  lines.push('', fs.readFileSync(path.join(root, 'findings.md'), 'utf8').trim());
}
lines.push('', '## Plans With Metrics', '',
  'Median-near measured executions. Every execution plan is retained.', '',
  '| Query | Configuration | Baseline | Global hash |',
  '| --- | --- | --- | --- |');
for (const q of queries) for (const c of cases) {
  const a = get('baseline', q, c), b = get('candidate', q, c);
  // Reference links keep the Markdown source readable.
  const key = `${q}-${c}`;
  lines.push(`| ${q} | ${c} | [plan][a-${key}] | [plan][b-${key}] |`);
}
lines.push('');
for (const q of queries) for (const c of cases) {
  for (const [binary, prefix] of [['baseline', 'a'], ['candidate', 'b']]) {
    lines.push(`[${prefix}-${q}-${c}]: ${get(binary, q, c).plan}`);
  }
}
lines.push('', '## Raw Data', '',
  '- [Summary JSON](summary.json), including block means and shuffle bytes.',
  '- [Baseline block 1](baseline-1/samples.json) and [block 2](baseline-2/samples.json).',
  '- [Candidate block 1](candidate-1/samples.json) and [block 2](candidate-2/samples.json).',
  '- [Reproduction driver](run-abba.mjs) and [analysis script](summarize.mjs).', '',
  'These selected queries cannot establish a full-suite performance improvement.', '');
fs.writeFileSync(path.join(root, 'report.md'), lines.join('\n'));
const metadataFile = path.join(root, 'metadata.json');
const metadata = JSON.parse(fs.readFileSync(metadataFile));
metadata.finished_at = new Date().toISOString();
metadata.validation = {
  executions: 288, measured: 240, warmups: 48,
  all_results_identical: true, all_plans_have_metrics: true,
  integration: '23 passed on full rerun; intermittent union completed-report snapshot failure remains',
  clippy: 'library and dynamic-filter integration target passed with -D warnings',
  format: 'cargo fmt --all --check passed', diff_check: 'git diff --check passed',
};
fs.writeFileSync(metadataFile, JSON.stringify(metadata, null, 2) + '\n');
console.table(summary.map(s => ({ binary: s.binary, query: s.query, case: s.case,
  ms: f(s.mean_ms), rows_M: million(s.forwarded_rows), merges: s.global_merges })));
