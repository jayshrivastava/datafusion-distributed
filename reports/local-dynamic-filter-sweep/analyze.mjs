import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(fileURLToPath(import.meta.url));
const manifest = JSON.parse(fs.readFileSync(path.join(root, 'manifest.json')));
const mean = xs => xs.reduce((a, b) => a + b, 0) / xs.length;
const median = xs => {
  const sorted = [...xs].sort((a, b) => a - b), middle = Math.floor(xs.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
};
const number = n => Number.isFinite(n) ? n.toFixed(1) : '-';
const sum = (s, key, predicate = () => true) => s.metrics.filter(predicate)
  .reduce((n, node) => n + (node.values[key] ?? 0), 0);
const scan = node => ['DistributedLeafExec', 'DataSourceExec'].includes(node.node);
const network = node => /^Network/.test(node.node);
const join = node => node.node.includes('JoinExec');

function totals(s) {
  return {
    result_rows: s.rows.length,
    scan_rows: sum(s, 'output_rows', scan),
    scanned_bytes: sum(s, 'bytes_scanned', scan),
    parquet_pruned_rows: sum(s, 'pushdown_rows_pruned', scan),
    dynamic_pruned_row_groups: sum(s, 'row_groups_pruned_dynamic_filter', scan),
    parquet_eval_ms: sum(s, 'row_pushdown_eval_time', scan) / 1e6,
    network_rows: sum(s, 'output_rows', network),
    network_bytes: sum(s, 'bytes_transferred', network),
    join_input_rows: sum(s, 'input_rows', join) + sum(s, 'build_input_rows', join),
    join_output_rows: sum(s, 'output_rows', join),
    join_compute_ms: sum(s, 'elapsed_compute', join) / 1e6,
    filter_output_rows: sum(s, 'output_rows', n => n.node === 'FilterExec'),
    sort_compute_ms: sum(s, 'elapsed_compute', n => n.node.startsWith('SortExec')) / 1e6,
    spilled_bytes: sum(s, 'spilled_bytes'),
    remote_updates: sum(s, 'dynamic_filter_updates_received'),
    global_hash_merges: sum(s, 'dynamic_filter_global_hash_merges'),
    dynamic_consumer_nodes: s.metrics.filter(n => n.dynamic_filter
      && (scan(n) || n.node === 'FilterExec')).length,
    predicate_errors: sum(s, 'num_predicate_creation_errors', scan)
      + sum(s, 'predicate_evaluation_errors', scan),
  };
}

function compare(sample, reference) {
  if (!reference) return 'no_control';
  if (JSON.stringify(sample.types) !== JSON.stringify(reference.types)
      || sample.rows.length !== reference.rows.length) return 'different';
  if (JSON.stringify(sample.rows) === JSON.stringify(reference.rows)) return 'exact';
  const floats = sample.types.map(t => /^Float/.test(t));
  const indices = floats.flatMap((v, i) => v ? [] : [i])
    .concat(floats.flatMap((v, i) => v ? [i] : []));
  const order = (a, b) => {
    for (const i of indices) {
      if (a[i] === b[i]) continue;
      if (floats[i] && a[i] && b[i] && Number.isFinite(+a[i]) && Number.isFinite(+b[i])) {
        return +a[i] - +b[i];
      }
      return a[i] < b[i] ? -1 : 1;
    }
    return 0;
  };
  const left = [...sample.rows].sort(order), right = [...reference.rows].sort(order);
  for (let r = 0; r < left.length; r++) for (let c = 0; c < floats.length; c++) {
    const a = left[r][c], b = right[r][c];
    if (a === b) continue;
    if (!floats[c] || !a || !b || !Number.isFinite(+a) || !Number.isFinite(+b)
        || Math.abs(+a - +b) > 1e-12 * Math.max(1, Math.abs(+a), Math.abs(+b))) {
      return 'different';
    }
  }
  return 'float_tolerance';
}

const loaded = [];
for (const block of manifest.blocks.filter(b => b.finished)) {
  const directory = path.join(root, 'runs', block.id);
  const files = fs.readdirSync(directory).filter(f => /-s\d+\.json$/.test(f));
  loaded.push({ block, files });
}
const controls = new Map();
for (const { block, files } of loaded) for (const file of files) {
  const match = file.match(/-Control-(on|off)-00-s(\d+)\.json$/);
  if (match) {
    const artifact = `runs/${block.id}/${file}`;
    controls.set(`${block.id}/${match[2]}`, artifact);
    const key = `${block.suite}/${block.query}/${match[2]}`;
    if (!controls.has(key) || match[1] === 'off') controls.set(key, artifact);
  }
}
let cachedReference;
function reference(artifact) {
  if (!artifact) return undefined;
  if (cachedReference?.artifact !== artifact) {
    cachedReference = { ...JSON.parse(fs.readFileSync(path.join(root, artifact))), artifact };
  }
  return cachedReference;
}
const groups = new Map();
const mismatches = [];
for (const { block, files } of loaded) for (const file of files) {
  const s = { ...JSON.parse(fs.readFileSync(path.join(root, 'runs', block.id, file))),
    artifact: `runs/${block.id}/${file}`, block: block.id, suite: block.suite };
  const phase = block.id.startsWith('confirm-') ? 'confirm' : 'screen';
  const key = `${phase}/${s.suite}/${s.query}/${s.case.name}`;
  if (!groups.has(key)) groups.set(key, { phase, suite: s.suite, query: s.query,
    case: s.case.name, samples: [], blocks: new Set(), validation: {} });
  const group = groups.get(key);
  const ref = reference((phase === 'confirm' && controls.get(`${s.block}/${s.statement}`))
    || controls.get(`${s.suite}/${s.query}/${s.statement}`));
  const validation = compare(s, ref);
  group.validation[validation] = (group.validation[validation] ?? 0) + 1;
  if (validation === 'different') mismatches.push({ artifact: s.artifact,
    reference: ref.artifact, rows: s.rows.length, reference_rows: ref.rows.length });
  group.blocks.add(block.id);
  if (s.iteration > 0) {
    group.samples.push({ iteration: s.iteration, block: s.block,
      ms: s.execution_ms, planning_ms: s.planning_ms, artifact: s.artifact,
      plan: `runs/${block.id}/${s.plan}`, display_error: s.display_error,
      totals: totals(s) });
  }
}
const summaries = [];
for (const group of groups.values()) {
  const { samples } = group;
  if (!samples.length) continue;
  const representative = [...samples].sort((a, b) => a.ms - b.ms)[Math.floor(samples.length / 2)];
  summaries.push({ phase: group.phase, suite: group.suite, query: group.query,
    case: group.case, n: samples.length, mean_ms: mean(samples.map(s => s.ms)),
    median_ms: median(samples.map(s => s.ms)), min_ms: Math.min(...samples.map(s => s.ms)),
    max_ms: Math.max(...samples.map(s => s.ms)),
    sd_ms: Math.sqrt(mean(samples.map(s => (s.ms - mean(samples.map(x => x.ms))) ** 2))),
    planning_ms: mean(samples.map(s => s.planning_ms)), validation: group.validation,
    display_errors: samples.filter(s => s.display_error).length,
    metrics: Object.fromEntries(Object.keys(representative.totals)
      .map(k => [k, mean(samples.map(s => s.totals[k]))])),
    representative_plan: representative.plan,
    blocks: [...group.blocks],
    samples,
  });
}
function workEvidence(control, treatment) {
  const a = control.metrics, b = treatment.metrics;
  const reductions = {};
  for (const key of ['scan_rows', 'network_rows', 'join_input_rows', 'join_output_rows',
    'filter_output_rows']) {
    if (a[key] > 0 && b[key] < a[key] * 0.99) reductions[key] = 1 - b[key] / a[key];
  }
  if (b.dynamic_pruned_row_groups > a.dynamic_pruned_row_groups) {
    reductions.dynamic_pruned_row_groups = b.dynamic_pruned_row_groups - a.dynamic_pruned_row_groups;
  }
  return { observed: b.dynamic_consumer_nodes > 0 && Object.keys(reductions).length > 0,
    reductions };
}
function bootstrap(pairs) {
  if (!pairs.length) return [0, 0];
  let state = 25092026;
  const random = () => { state = (1664525 * state + 1013904223) >>> 0; return state / 2 ** 32; };
  const rounds = [...new Set(pairs.map(p => p.round))].map(r => pairs.filter(p => p.round === r));
  const ratios = [];
  for (let b = 0; b < 20000; b++) {
    let off = 0, on = 0;
    for (const round of rounds) for (let i = 0; i < round.length; i++) {
      const pair = round[Math.floor(random() * round.length)]; off += pair.off; on += pair.on;
    }
    ratios.push(off / on);
  }
  ratios.sort((a, b) => a - b);
  return [ratios[500], ratios[19500]];
}
const comparisons = [];
function matchingControl(treatment, mode) {
  const control = summaries.find(s => s.phase === treatment.phase && s.suite === treatment.suite
    && s.query === treatment.query && s.case === `Control-${mode}`);
  if (!control || treatment.phase !== 'confirm') return control;
  const allowed = new Set(treatment.blocks);
  const samples = control.samples.filter(s => allowed.has(s.block));
  if (!samples.length) return undefined;
  return { ...control, n: samples.length, samples,
    mean_ms: mean(samples.map(s => s.ms)), median_ms: median(samples.map(s => s.ms)),
    representative_plan: [...samples].sort((a, b) => a.ms - b.ms)[Math.floor(samples.length / 2)].plan,
    metrics: Object.fromEntries(Object.keys(control.metrics)
      .map(k => [k, mean(samples.map(s => s.totals[k]))])) };
}
function pairedSamples(control, treatment) {
  return treatment.samples.map(on => ({ round: on.block, on: on.ms,
    off: control.samples.find(off => off.block === on.block && off.iteration === on.iteration)?.ms,
  })).filter(p => Number.isFinite(p.off));
}
for (const treatment of summaries.filter(s => !s.case.startsWith('Control'))) {
  const mode = treatment.case.split('-')[1];
  const control = matchingControl(treatment, mode);
  if (!control) continue;
  const otherControl = matchingControl(treatment, mode === 'on' ? 'off' : 'on');
  const bestControl = otherControl && otherControl.mean_ms < control.mean_ms ? otherControl : control;
  const evidence = workEvidence(control, treatment);
  const result = { phase: treatment.phase, suite: treatment.suite, query: treatment.query,
    case: treatment.case, control_ms: control.mean_ms, treatment_ms: treatment.mean_ms,
    speedup: control.mean_ms / treatment.mean_ms,
    best_disabled_case: bestControl.case, best_disabled_ms: bestControl.mean_ms,
    best_disabled_plan: bestControl.representative_plan,
    best_disabled_speedup: bestControl.mean_ms / treatment.mean_ms,
    median_speedup: control.median_ms / treatment.median_ms, evidence,
    valid_results: !treatment.validation.different && !treatment.validation.no_control
      && !control.validation.different && !control.validation.no_control,
    control_plan: control.representative_plan, treatment_plan: treatment.representative_plan,
    control_metrics: control.metrics, treatment_metrics: treatment.metrics,
    display_errors: treatment.display_errors,
  };
  if (treatment.phase === 'confirm') {
    const pairs = pairedSamples(control, treatment);
    result.pairs = pairs.length; result.faster_pairs = pairs.filter(p => p.on < p.off).length;
    result.interval95 = bootstrap(pairs);
    result.best_disabled_interval95 = bootstrap(pairedSamples(bestControl, treatment));
    result.round_speedups = Object.fromEntries([...new Set(pairs.map(p => p.round))].map(r => {
      const ps = pairs.filter(p => p.round === r);
      return [r, mean(ps.map(p => p.off)) / mean(ps.map(p => p.on))];
    }));
    result.qualified = result.valid_results && evidence.observed && !result.display_errors
      && treatment.metrics.predicate_errors === 0
      && result.speedup > 1 && result.median_speedup > 1 && result.interval95[0] > 1
      && pairs.length >= 20 && Object.values(result.round_speedups).every(r => r > 1);
  }
  comparisons.push(result);
}
comparisons.sort((a, b) => b.speedup - a.speedup);
const errors = loaded.flatMap(({ block }) => (block.errors ?? []).map(file => ({
  block: block.id, ...JSON.parse(fs.readFileSync(path.join(root, 'runs', block.id, file))),
})));
const caseNames = ['Control-off', 'A-off', 'B-off', 'Control-on', 'A-on', 'B-on'];
const screenEntries = summaries.filter(s => s.phase === 'screen');
const expectedQueries = Object.values(manifest.suites).reduce((n, s) => n + s.queries.length, 0);
const complete = s => s && s.n === manifest.screening_iterations;
const coverage = {
  expected_queries: expectedQueries,
  expected_matrix_entries: expectedQueries * caseNames.length,
  attempted_matrix_entries: loaded.filter(({ block }) => block.id.startsWith('screen-'))
    .reduce((n, { block }) => n + block.cases.length, 0),
  measured_matrix_entries: screenEntries.length,
  complete_matrix_entries: screenEntries.filter(complete).length,
  validated_complete_matrix_entries: screenEntries.filter(s => complete(s)
    && !s.validation.different && !s.validation.no_control).length,
  measured_screen_executions: screenEntries.reduce((n, s) => n + s.n, 0),
  measured_confirmation_executions: summaries.filter(s => s.phase === 'confirm')
    .reduce((n, s) => n + s.n, 0),
  failed_process_blocks: loaded.filter(({ block }) => block.code !== null && block.code !== 0)
    .map(({ block }) => block.id),
  unobserved_process_exits: loaded.filter(({ block }) => block.code === null)
    .map(({ block }) => block.id),
};
const suiteTotals = Object.fromEntries(Object.entries(manifest.suites).map(([suite, spec]) => {
  const included = [], excluded = [];
  for (const query of spec.queries) {
    const entries = caseNames.map(c => screenEntries.find(s => s.suite === suite && s.query === query && s.case === c));
    (entries.every(s => complete(s) && !s.validation.different && !s.validation.no_control)
      ? included : excluded).push(query);
  }
  const totals = Object.fromEntries(caseNames.map(c => [c, screenEntries
    .filter(s => s.suite === suite && s.case === c && included.includes(s.query))
    .reduce((n, s) => n + s.mean_ms, 0)]));
  return [suite, { included, excluded, summed_mean_ms: totals }];
}));
const summary = { updated: new Date().toISOString(), blocks: loaded.length,
  coverage, suite_totals: suiteTotals, summaries, comparisons, errors, mismatches };
fs.writeFileSync(path.join(root, 'summary.json'), JSON.stringify(summary, null, 2) + '\n');

const lines = ['# Local Dynamic-Filter Matrix', '',
  'Generated from raw local executions. Timings are milliseconds; Q18 (TPC-H) is excluded.', '',
  'Parquet pushdown and reordering are both off/on. Compare within a matching mode.', '',
  'Screening has three measured samples after one warmup per entry. Missing or failed',
  'entries are shown explicitly. Screening speedups are candidates, not conclusions.', '',
  `Completed process blocks: ${loaded.length}; errors: ${errors.length};`,
  `nonidentical result samples: ${mismatches.length}.`, '',
  `Complete matrix entries: ${coverage.complete_matrix_entries}/${coverage.expected_matrix_entries}.`,
  `Measured screening executions: ${coverage.measured_screen_executions}.`, ''];
for (const [suite, spec] of Object.entries(manifest.suites)) {
  lines.push(`## ${suite}`, '', '| Query | Ctrl-off | A-off | B-off | Ctrl-on | A-on | B-on |',
    '| --- | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const query of spec.queries) {
    lines.push(`| ${query} | ${caseNames.map(c => {
      const entry = summaries.find(s => s.phase === 'screen' && s.suite === suite && s.query === query && s.case === c);
      const error = errors.find(e => e.block.startsWith(`screen-${suite}-${query}-`)
        && e.case.name === c);
      if (!entry) return error ? (/Timed out/.test(error.error) ? 'timeout' : 'error') : '-';
      return number(entry.mean_ms) + (entry.validation.different ? '*' : '')
        + (entry.validation.no_control ? '?' : '') + (complete(entry) ? '' : `(n=${entry.n})`);
    }).join(' | ')} |`);
  }
  lines.push('');
  const total = suiteTotals[suite];
  if (total.included.length) {
    lines.push(`Comparable-query total (${total.included.length}/${spec.queries.length} queries):`, '',
      '| Ctrl-off | A-off | B-off | Ctrl-on | A-on | B-on |',
      '| ---: | ---: | ---: | ---: | ---: | ---: |',
      `| ${caseNames.map(c => number(total.summed_mean_ms[c])).join(' | ')} |`, '');
  }
  if (total.excluded.length) {
    lines.push('Excluded from totals because results differ or measurements are incomplete:');
    for (let i = 0; i < total.excluded.length; i += 12) {
      lines.push(total.excluded.slice(i, i + 12).join(', ') + (i + 12 < total.excluded.length ? ',' : '.'));
    }
    lines.push('');
  }
}
lines.push('## Notes', '', '`*` means at least one result differs from its control; investigate before',
  'claiming a performance win. `?` means no completed control result is available.',
  'Float results alone allow relative tolerance 1e-12. Incomplete sample counts',
  'are shown as `(n=...)`; `-` means no finalized measurement or recorded error.', '',
  'The generic numeric export does not preserve composite PruningMetrics/Ratio',
  'values. Use the rendered plans for statistics/page-index pruning counters;',
  'their zero placeholders in raw JSON do not mean no pruning occurred.', '',
  'Raw plans, result values, and all numeric metrics are under `runs/`.',
  'See `summary.json` for variance, failures, result comparisons, and paired intervals.', '');
fs.writeFileSync(path.join(root, 'matrix.md'), lines.join('\n'));
const candidates = comparisons.filter(c => c.phase === 'screen' && c.valid_results
  && c.evidence.observed && c.speedup > 1.03);
console.log(JSON.stringify({ blocks: loaded.length, errors: errors.length,
  mismatches: mismatches.length,
  candidates: candidates.map(({ suite, query, case: c, speedup, evidence }) => ({ suite, query, case: c, speedup, evidence })),
  confirmed: comparisons.filter(c => c.phase === 'confirm').map(({ suite, query, case: c,
    speedup, interval95, qualified }) => ({ suite, query, case: c, speedup, interval95, qualified })),
}, null, 2));
