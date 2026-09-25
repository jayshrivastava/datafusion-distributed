import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(fileURLToPath(import.meta.url));
const manifest = JSON.parse(fs.readFileSync(path.join(root, 'manifest.json')));
assert.equal(manifest.status, 'complete');
const mean = xs => xs.reduce((a, b) => a + b, 0) / xs.length;
const percentile = (xs, p) => {
  const sorted = [...xs].sort((a, b) => a - b);
  const i = (sorted.length - 1) * p;
  return sorted[Math.floor(i)] + (sorted[Math.ceil(i)] - sorted[Math.floor(i)]) * (i % 1);
};
const metric = (s, key, predicate = () => true) => s.metrics.filter(predicate)
  .reduce((n, m) => n + (m.values[key] ?? 0), 0);
const scan = m => ['DataSourceExec', 'DistributedLeafExec'].includes(m.node);
const network = m => m.node.startsWith('Network');
const counters = s => ({
  scan_rows: metric(s, 'output_rows', scan),
  scan_bytes: metric(s, 'bytes_scanned', scan),
  parquet_rows_pruned: metric(s, 'pushdown_rows_pruned', scan),
  dynamic_row_groups_pruned: metric(s, 'row_groups_pruned_dynamic_filter', scan),
  parquet_eval_ms: metric(s, 'row_pushdown_eval_time', scan) / 1e6,
  network_rows: metric(s, 'output_rows', network),
  network_bytes: metric(s, 'bytes_transferred', network),
  join_compute_ms: metric(s, 'elapsed_compute', m => m.node === 'HashJoinExec') / 1e6,
  spilled_bytes: metric(s, 'spilled_bytes'),
  remote_updates: metric(s, 'dynamic_filter_updates_received'),
  global_hash_merges: metric(s, 'dynamic_filter_global_hash_merges'),
});
const samples = manifest.rounds.flatMap(round => {
  assert(round.validated);
  return JSON.parse(fs.readFileSync(path.join(root, round.directory, 'samples.json')))
    .map(s => ({ ...s, directory: round.directory, counters: counters(s) }));
});
assert.equal(samples.length, 126);
const measured = samples.filter(s => s.iteration > 0);
assert.equal(measured.length, 120);
function stats(rows) {
  const times = rows.map(s => s.execution_ms);
  const average = mean(times);
  const median = percentile(times, 0.5);
  const representative = rows.reduce((a, b) => Math.abs(a.execution_ms - median)
    <= Math.abs(b.execution_ms - median) ? a : b);
  return {
    n: rows.length, mean_ms: average, median_ms: median,
    p90_ms: percentile(times, 0.9), min_ms: Math.min(...times), max_ms: Math.max(...times),
    sd_ms: Math.sqrt(times.reduce((n, t) => n + (t - average) ** 2, 0) / (times.length - 1)),
    planning_ms: mean(rows.map(s => s.planning_ms)),
    counters: Object.fromEntries(Object.keys(rows[0].counters)
      .map(k => [k, mean(rows.map(s => s.counters[k]))])),
    plan: `${representative.directory}/${representative.plan}`,
  };
}
const rounds = manifest.rounds.map(r => {
  const rows = measured.filter(s => s.directory === r.directory);
  const off = stats(rows.filter(s => !s.case.dynamic_filtering));
  const on = stats(rows.filter(s => s.case.dynamic_filtering));
  const pairs = Array.from({ length: 20 }, (_, i) => {
    const pair = rows.filter(s => s.iteration === i + 1);
    assert.equal(pair.length, 2);
    return {
      off: pair.find(s => !s.case.dynamic_filtering).execution_ms,
      on: pair.find(s => s.case.dynamic_filtering).execution_ms,
    };
  });
  return { directory: r.directory, off, on, pairs, speedup: off.mean_ms / on.mean_ms,
    wins: pairs.filter(p => p.on < p.off).length };
});
const off = stats(measured.filter(s => !s.case.dynamic_filtering));
const on = stats(measured.filter(s => s.case.dynamic_filtering));
const speedup = off.mean_ms / on.mean_ms;
const wins = rounds.reduce((n, r) => n + r.wins, 0);
let seed = 20260924;
const random = n => {
  seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
  return Math.floor(seed / 2 ** 32 * n);
};
const bootstrap = Array.from({ length: 20000 }, () => {
  let baseline = 0, candidate = 0;
  for (const round of rounds) for (let i = 0; i < round.pairs.length; i++) {
    const pair = round.pairs[random(round.pairs.length)];
    baseline += pair.off; candidate += pair.on;
  }
  return baseline / candidate;
});
const confidence = [percentile(bootstrap, 0.025), percentile(bootstrap, 0.975)];
const checks = {
  each_round_at_least_1_20x: rounds.every(r => r.speedup >= 1.2),
  at_least_90_percent_pairs_faster: wins >= 54,
  bootstrap_lower_bound_above_1_10x: confidence[0] > 1.1,
  all_results_match: manifest.rounds.every(r => r.validated),
  reduced_scan_and_network_work: on.counters.scan_rows < off.counters.scan_rows * 0.9
    && on.counters.network_bytes < off.counters.network_bytes * 0.9,
};
const passed = Object.values(checks).every(Boolean);
fs.writeFileSync(path.join(root, 'summary.json'), JSON.stringify({
  off, on, rounds, speedup, wins, confidence, bootstrap_replicates: 20000,
  checks, passed,
}, null, 2) + '\n');
const f = n => n.toFixed(1);
const lines = ['# TPC-H Q18 SF10: Repeatability Result', '',
  passed ? '**All predeclared acceptance criteria passed.**'
    : '**Not all predeclared acceptance criteria passed.**', '',
  `Dynamic filtering off: **${f(off.mean_ms)} ms**; on: **${f(on.mean_ms)} ms**.`,
  `Mean speedup: **${speedup.toFixed(2)}x**, with ${wins}/60 paired executions faster.`,
  `Paired bootstrap 95% interval: **${confidence.map(n => n.toFixed(2)).join('-')}x**.`, '',
  'Same B/global-hash binary on both sides. Parquet row pushdown and',
  'reordering stay enabled; only dynamic filtering is toggled. No injected',
  'post-scan filter is active. All 126 executions, including warmups, return',
  'the same 624 rows and exact values as the saved SF10 reference.', '',
  '## Independent Worker Sessions', '',
  '| Session | Off mean ms | On mean ms | Speedup | Faster pairs |',
  '| --- | ---: | ---: | ---: | ---: |'];
for (const r of rounds) lines.push(
  `| ${r.directory} | ${f(r.off.mean_ms)} | ${f(r.on.mean_ms)} | ${r.speedup.toFixed(2)}x | ${r.wins}/20 |`);
lines.push('', 'Each session starts fresh worker processes and excludes one warmup per',
  'case. Each measured round contains both settings; order is balanced.', '',
  '## Latency Spread', '',
  '| Case | n | Mean ms | Median ms | P90 ms | SD ms | Min ms | Max ms |',
  '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
for (const [name, s] of [['Off', off], ['On', on]]) lines.push(
  `| ${name} | ${s.n} | ${f(s.mean_ms)} | ${f(s.median_ms)} | ${f(s.p90_ms)} | ${f(s.sd_ms)} | ${f(s.min_ms)} | ${f(s.max_ms)} |`);
lines.push('', '## Plan Evidence', '',
  'Means across the same measured executions, from numeric metrics rather',
  'than rounded display text. Operator times sum concurrent work, not wall time.', '',
  '| Metric | DF off | DF on |', '| --- | ---: | ---: |');
for (const [key, label, scale] of [
  ['scan_rows', 'Scan output, million rows', 1e6],
  ['scan_bytes', 'Scanned MB', 1e6],
  ['parquet_rows_pruned', 'Parquet rows pruned, millions', 1e6],
  ['dynamic_row_groups_pruned', 'Dynamic row groups pruned', 1],
  ['parquet_eval_ms', 'Parquet evaluation, summed ms', 1],
  ['network_rows', 'Network output, million rows', 1e6],
  ['network_bytes', 'Transferred MB', 1e6],
  ['join_compute_ms', 'Join compute, summed ms', 1],
  ['spilled_bytes', 'Spilled MB', 1e6],
  ['remote_updates', 'Remote filter updates', 1],
  ['global_hash_merges', 'Global-hash merges', 1],
]) lines.push(`| ${label} | ${f(off.counters[key] / scale)} | ${f(on.counters[key] / scale)} |`);
lines.push('', `Representative executed plans: [off](${off.plan}),`,
  `[on](${on.plan}). Each is closest to its case's pooled median.`, '',
  'Interpretation is in [findings.md](findings.md).', '',
  '## Reproduce', '',
  'Build from the recorded commit plus saved patch/untracked sources, or use',
  'the saved release binary. The runner starts and stops its four workers.', '',
  '```sh', './runner \\',
  '  --data /instance_storage/bits_cache/datafusion-benchmark-data/tpch/sf10 \\',
  '  --output /tmp/q18-repeat-new \\',
  '  --queries q18 --paired-dynamic-filtering --iterations 20', '```', '',
  'Repeat with new output directories and `--reverse-order` on alternate',
  'sessions. All settings, result values, per-execution plans, source hashes,',
  'and timing samples are retained here. Do not overwrite earlier sessions.', '',
  '## Limits', '',
  '- One selected query and one machine; this is not a suite-wide claim.',
  '- Warm local SF10 files, not S3 or a cold-cache test.',
  '- Four workers share 16 ARM cores; this is not twelve separate machines.',
  '- Planning/rendering are outside the execution timer; planning is recorded.',
  '- Local and remote dynamic filtering share the master switch.',
  '- Confidence intervals resample pairs within sessions. They do not model',
  '  variability across machines or arbitrary long-term system drift.',
  '- No new SF100 claim: an earlier remote run did not reproduce the gain.', '',
  '[Predeclared plan](plan.md), [raw summary](summary.json),',
  '[provenance](manifest.json), [harness](source/benchmarks/src/bin/tpch-filter-matrix.rs).', '');
fs.writeFileSync(path.join(root, 'report.md'), lines.join('\n'));
console.log(JSON.stringify({ passed, speedup, wins, confidence,
  rounds: rounds.map(r => ({ session: r.directory, speedup: r.speedup, wins: r.wins })) }, null, 2));
