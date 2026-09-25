import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(fileURLToPath(import.meta.url));
const summary = JSON.parse(fs.readFileSync(path.join(root, 'summary.json')));
const directory = path.join(root, 'runs', 'selected');
fs.mkdirSync(directory, { recursive: true });

function nodes(group, blocks) {
  const samples = group.samples.filter(s => blocks.has(s.block));
  const entries = new Map();
  for (const sample of samples) {
    const raw = JSON.parse(fs.readFileSync(path.join(root, sample.artifact)));
    for (const node of raw.metrics) {
      const key = JSON.stringify([node.index, node.node, node.columns]);
      if (!entries.has(key)) entries.set(key, {
        index: node.index, node: node.node, columns: node.columns,
        observations: 0, dynamic_filter_observations: 0, values: {},
      });
      const entry = entries.get(key);
      entry.observations++;
      entry.dynamic_filter_observations += Number(node.dynamic_filter);
      for (const [key, value] of Object.entries(node.values)) {
        entry.values[key] = (entry.values[key] ?? 0) + value;
      }
    }
  }
  return [...entries.values()].map(entry => ({ ...entry,
    values: Object.fromEntries(Object.entries(entry.values)
      .map(([key, sum]) => [key, sum / samples.length])),
  }));
}

const seen = new Set();
const evidence = summary.comparisons.filter(c => c.phase === 'confirm').map(c => {
  const stem = `${c.suite}-${c.query}-${c.case}`;
  const group = caseName => summary.summaries.find(s => s.phase === 'confirm'
    && s.suite === c.suite && s.query === c.query && s.case === caseName);
  const treatment = group(c.case);
  const blocks = new Set(treatment.blocks);
  const plans = {};
  for (const [role, original] of Object.entries({
    control: c.control_plan, enabled: c.treatment_plan,
    best_disabled: c.best_disabled_plan,
  })) {
    const copied = `runs/selected/${stem}-${role}.txt`;
    fs.copyFileSync(path.join(root, original), path.join(root, copied));
    plans[role] = { original, copied };
  }
  const sql = `runs/selected/${stem}.sql`;
  fs.copyFileSync(path.join(root, 'runs', treatment.blocks[0], `${c.query}.sql`),
    path.join(root, sql));
  const key = `${c.suite}/${c.query}`;
  const eligible = c.qualified && c.best_disabled_interval95[0] > 1;
  const first = eligible && !seen.has(key);
  if (first) seen.add(key);
  return {
    ...c, headline_rank: first && seen.size <= 10 ? seen.size : null,
    plans, sql, measured_per_case: treatment.n,
    control_nodes: nodes(group(`Control-${c.case.split('-')[1]}`), blocks),
    enabled_nodes: nodes(treatment, blocks),
  };
});

fs.writeFileSync(path.join(root, 'evidence.json'), JSON.stringify({
  summary_updated: summary.updated,
  note: 'Numeric node values are means across the selected confirmation blocks. '
    + 'PruningMetrics/Ratio zeros in JSON are placeholders; use the text plans. '
    + 'Copied plans are median-latency examples, not averages.',
  comparisons: evidence,
}, null, 2) + '\n');
console.table(evidence.map(e => ({
  rank: e.headline_rank, query: `${e.suite}/${e.query}`, case: e.case,
  speedup: e.speedup.toFixed(3), best_disabled: e.best_disabled_speedup.toFixed(3),
  samples: e.measured_per_case,
  plan_shape_changes: [...e.control_nodes, ...e.enabled_nodes]
    .some(n => n.observations !== e.measured_per_case),
})));
