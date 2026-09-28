import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { gunzipSync, gzipSync } from 'node:zlib';

const tools = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const root = path.resolve(tools, '../datafusion-distributed/reports/remote-tpcds-q80-q27-scan-probes');
const manifest = JSON.parse(fs.readFileSync(path.join(root, 'manifest.json')));
const mean = xs => xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
const median = xs => {
  const sorted = [...xs].sort((a, b) => a - b);
  return (sorted[Math.floor((sorted.length - 1) / 2)] + sorted[Math.floor(sorted.length / 2)]) / 2;
};
const fmt = (n, digits = 1) => n == null ? '-' : n.toLocaleString('en-US',
  { minimumFractionDigits: digits, maximumFractionDigits: digits });
function numeric(text, key) {
  const match = /^([\d.]+)\s*(ns|\u00b5s|us|ms|s|KB|MB|GB|TB|K|M|B|T)?$/.exec(text.trim());
  if (!match) return null;
  const n = Number(match[1]), unit = match[2];
  const times = { ns: 1e-6, '\u00b5s': 1e-3, us: 1e-3, ms: 1, s: 1000 };
  if (unit in times) return n * times[unit];
  if (/^[KMGT]B$/.test(unit)) return n * 1024 ** ('KMGT'.indexOf(unit[0]) + 1);
  if (unit === 'B' && key !== 'bytes_scanned' && /bytes|mem_used/.test(key)) return n;
  return n * ({ K: 1e3, M: 1e6, B: 1e9, T: 1e12 }[unit] ?? 1);
}
function metricMap(line) {
  const result = {};
  for (const [, name, body] of line.matchAll(/\b([a-z_0-9]+)=\{([^}]+)\}/g)) {
    const values = {};
    for (const entry of body.split(',')) {
      const colon = entry.indexOf(':');
      if (colon === -1) continue;
      const task = entry.slice(0, colon).trim();
      const text = entry.slice(colon + 1).trim();
      const pruning = /(.+?) total \u2192 (.+?) matched/.exec(text);
      const value = pruning ? numeric(pruning[1], name) - numeric(pruning[2], name) : numeric(text, name);
      if (value !== null && Number.isFinite(value)) values[task] = value;
    }
    if (Object.keys(values).length) result[name] = values;
  }
  for (const [, name, text] of line.matchAll(/\b([a-z_0-9]+)=([\d.]+\s*(?:ns|\u00b5s|us|ms|s|KB|MB|GB|TB|K|M|B|T)?)(?=,|$)/g)) {
    const value = numeric(text, name);
    if (value !== null && !result[name]) result[name] = { 0: value };
  }
  return result;
}
const sumMetric = (map, key) => Object.values(map[key] ?? {}).reduce((a, b) => a + b, 0);
function parsePlan(text) {
  let current = 'DistributedExec';
  const stages = {}, nodes = [];
  for (const [index, line] of text.split('\n').entries()) {
    const header = /\u250c\u2500+ (Stage \d+|DistributedExec)/.exec(line);
    if (header) {
      current = header[1];
      stages[current] = { line: index + 1, metrics: metricMap(line), nodes: [] };
      continue;
    }
    const kind = /\b([A-Za-z]+Exec)(?::|\b)/.exec(line)?.[1];
    if (!kind) continue;
    const node = { stage: current, line: index + 1, kind,
      table: /tpcds\/sf\d+\/([^/]+)\//.exec(line)?.[1], metrics: metricMap(line) };
    for (const name of ['file_open_errors', 'file_scan_errors', 'num_predicate_creation_errors', 'predicate_evaluation_errors']) {
      assert.equal(sumMetric(node.metrics, name), 0, `Nonzero ${name}`);
    }
    stages[current].nodes.push(node);
    nodes.push(node);
  }
  const total = (predicate, names) => nodes.filter(predicate)
    .reduce((sum, node) => sum + names.reduce((n, key) => n + sumMetric(node.metrics, key), 0), 0);
  const scan = key => total(n => n.kind === 'DataSourceExec', [key]);
  const coordinator = stages.DistributedExec.metrics;
  const probeNames = [...new Set(nodes.flatMap(node => Object.keys(node.metrics)))]
    .filter(name => name.startsWith('probe_'));
  const tasks = Object.entries(stages).flatMap(([stage, data]) =>
    Object.keys(data.metrics.output_streams_started ?? {}).map(task => {
      const value = name => data.metrics[name]?.[task] ?? 0;
      return { stage, task, stage_line: data.line,
        tables: [...new Set(data.nodes.filter(n => n.table && task in (n.metrics.output_rows ?? {})).map(n => n.table))],
        executed_ms: value('plan_executed_at'), first_output_ms: value('output_first_batch_at'),
        last_output_ms: value('output_last_batch_at'), output_eos_ms: value('output_stream_finished_at'),
        channel_closed_ms: value('plan_finished_at'), streams_started: value('output_streams_started'),
        streams_completed: value('output_streams_completed'), poll_ms: value('output_poll_time'),
        max_poll_ms: value('output_poll_max'), filter_first_ms: value('dynamic_filter_first_applied_at'),
        filter_last_ms: value('dynamic_filter_last_applied_at'), filter_apply_ms: value('dynamic_filter_apply_time'),
        filter_updates: value('dynamic_filter_updates_applied'),
        probes: Object.fromEntries(probeNames.map(name => [name,
          data.nodes.reduce((sum, node) => sum + (node.metrics[name]?.[task] ?? 0), 0)])),
      };
    }));
  const summaries = Object.fromEntries(Object.entries(stages).map(([stage, data]) => {
    const stageTasks = tasks.filter(t => t.stage === stage);
    const metrics = Object.fromEntries(Object.entries(data.metrics).map(([name, values]) => [name,
      /_at$|_max$/.test(name) ? Math.max(...Object.values(values)) : sumMetric(data.metrics, name)]));
    return [stage, { line: data.line, tables: [...new Set(data.nodes.map(n => n.table).filter(Boolean))],
      metrics, tasks: stageTasks,
      probes: Object.fromEntries(probeNames.map(name => [name,
        data.nodes.reduce((sum, node) => sum + sumMetric(node.metrics, name), 0)])),
      timing: {
        first_output_mean_ms: mean(stageTasks.filter(t => t.first_output_ms > 0).map(t => t.first_output_ms)),
        first_output_min_ms: Math.min(...stageTasks.filter(t => t.first_output_ms > 0).map(t => t.first_output_ms)),
        output_eos_max_ms: Math.max(0, ...stageTasks.map(t => t.output_eos_ms)),
        first_to_eos_mean_ms: mean(stageTasks.filter(t => t.first_output_ms > 0 && t.output_eos_ms > 0)
          .map(t => t.output_eos_ms - t.first_output_ms)),
        filter_first_mean_ms: mean(stageTasks.filter(t => t.filter_updates > 0).map(t => t.filter_first_ms)),
        filter_last_mean_ms: mean(stageTasks.filter(t => t.filter_updates > 0).map(t => t.filter_last_ms)),
      } }];
  }));
  const totals = {
    ...Object.fromEntries(probeNames.map(name => [name, total(() => true, [name])])),
    scan_rows: scan('output_rows'), scan_bytes: scan('bytes_scanned'),
    scan_open_ms: scan('time_elapsed_opening'), scan_scan_ms: scan('time_elapsed_scanning_total'),
    scan_processing_ms: scan('time_elapsed_processing'), metadata_ms: scan('metadata_load_time'),
    row_predicate_ms: scan('row_pushdown_eval_time'), parquet_rows_pruned: scan('pushdown_rows_pruned'),
    parquet_rows_matched: scan('pushdown_rows_matched'), pages_rows_pruned: scan('page_index_rows_pruned'),
    dynamic_row_groups_pruned: scan('row_groups_pruned_dynamic_filter'),
    statistics_row_groups_pruned: scan('row_groups_pruned_statistics'),
    join_input_rows: total(n => n.kind === 'HashJoinExec', ['build_input_rows', 'input_rows']),
    join_compute_ms: total(n => n.kind === 'HashJoinExec', ['elapsed_compute']),
    network_bytes: total(n => /^Network/.test(n.kind), ['bytes_transferred']),
    network_rows: total(n => /^Network/.test(n.kind), ['output_rows']),
    filter_register_ms: sumMetric(coordinator, 'dynamic_filter_register_time'),
    filter_update_ms: sumMetric(coordinator, 'dynamic_filter_update_time'),
    filter_dispatch_ms: sumMetric(coordinator, 'dynamic_filter_dispatch_time'),
    filter_updates_received: sumMetric(coordinator, 'dynamic_filter_updates_received'),
    filter_updates_sent: sumMetric(coordinator, 'dynamic_filter_updates_sent'),
    filter_bytes_sent: sumMetric(coordinator, 'dynamic_filter_bytes_sent'),
    filter_apply_ms: tasks.reduce((n, t) => n + t.filter_apply_ms, 0),
    filter_apply_max_task_ms: Math.max(0, ...tasks.map(t => t.filter_apply_ms)),
    filter_updates_applied: tasks.reduce((n, t) => n + t.filter_updates, 0),
    output_eos_max_ms: Math.max(0, ...tasks.map(t => t.output_eos_ms)),
    output_poll_ms: tasks.reduce((n, t) => n + t.poll_ms, 0),
    output_poll_max_ms: Math.max(0, ...tasks.map(t => t.max_poll_ms)),
    streams_started: tasks.reduce((n, t) => n + t.streams_started, 0),
    streams_completed: tasks.reduce((n, t) => n + t.streams_completed, 0),
  };
  const tables = {};
  for (const table of new Set(nodes.map(n => n.table).filter(Boolean))) {
    const scans = nodes.filter(n => n.table === table && n.kind === 'DataSourceExec');
    const keys = ['output_rows', 'bytes_scanned', 'row_pushdown_eval_time', 'time_elapsed_opening',
      'time_elapsed_processing', 'time_elapsed_scanning_total', 'metadata_load_time', 'pushdown_rows_pruned',
      'pushdown_rows_matched', 'page_index_rows_pruned', 'row_groups_pruned_dynamic_filter',
      ...probeNames.filter(name => !name.startsWith('probe_coalesce_'))];
    tables[table] = Object.fromEntries(keys.map(key => [key, scans.reduce((n, scan) => n + sumMetric(scan.metrics, key), 0)]));
    tables[table].output_eos_max_ms = Math.max(0, ...tasks.filter(t => t.tables.includes(table)).map(t => t.output_eos_ms));
    tables[table].filter_first_max_ms = Math.max(0, ...tasks.filter(t => t.tables.includes(table)).map(t => t.filter_first_ms));
  }
  assert(totals.probe_range_read_calls >= totals.probe_first_batch_read_calls);
  assert(totals.probe_scan_streams >= totals.probe_scan_output_streams);
  assert(totals.probe_scan_poll_cpu > 0 && totals.probe_data_bytes_requested > 0);
  assert(totals.probe_coalesce_output_buckets >= totals.probe_coalesce_first_emit_at_eof);
  return { totals, tables, stages: summaries, nodes };
}
const groups = new Map();
for (const sample of manifest.samples.filter(s => s.kind === 'measured')) {
  const key = `${sample.workers}/${sample.query}/${sample.variant}`;
  const group = groups.get(key) ?? { workers: sample.workers, partitions: sample.partitions,
    query: sample.query, variant: sample.variant, samples: [] };
  const plan = gunzipSync(fs.readFileSync(path.join(root, sample.plan))).toString();
  group.samples.push({ ...sample, ...parsePlan(plan) });
  groups.set(key, group);
}
fs.mkdirSync(path.join(root, 'plans'), { recursive: true });
const averageObjects = objects => Object.fromEntries([...new Set(objects.flatMap(o => Object.keys(o)))]
  .map(key => [key, mean(objects.map(o => o[key]).filter(n => Number.isFinite(n)))]));
for (const group of groups.values()) {
  const times = group.samples.map(s => s.elapsed_ms);
  group.n = times.length;
  group.mean_ms = mean(times); group.median_ms = median(times);
  group.min_ms = Math.min(...times); group.max_ms = Math.max(...times);
  group.sd_ms = Math.sqrt(mean(times.map(t => (t - group.mean_ms) ** 2)) * times.length / Math.max(1, times.length - 1));
  group.sessions = Object.fromEntries([...new Set(group.samples.map(s => s.session))]
    .map(s => [s, mean(group.samples.filter(sample => sample.session === s).map(sample => sample.elapsed_ms))]));
  group.session_phases_ms = Object.fromEntries([...new Set(group.samples.map(s => s.session))]
    .map(s => [s, averageObjects(group.samples.filter(sample => sample.session === s).map(sample => sample.timings_ms))]));
  group.mean_phases_ms = averageObjects(group.samples.map(s => s.timings_ms));
  group.mean_metrics = averageObjects(group.samples.map(s => s.totals));
  group.mean_tasks = mean(group.samples.map(s => s.tasks));
  group.tables = Object.fromEntries([...new Set(group.samples.flatMap(s => Object.keys(s.tables)))]
    .map(table => [table, averageObjects(group.samples.map(s => s.tables[table] ?? {}))]));
  group.stages = Object.fromEntries([...new Set(group.samples.flatMap(s => Object.keys(s.stages)))]
    .map(stage => [stage, { tables: [...new Set(group.samples.flatMap(s => s.stages[stage]?.tables ?? []))],
      metrics: averageObjects(group.samples.map(s => s.stages[stage]?.metrics ?? {})),
      probes: averageObjects(group.samples.map(s => s.stages[stage]?.probes ?? {})),
      timing: averageObjects(group.samples.map(s => s.stages[stage]?.timing ?? {})) }]));
  const criticalTasks = group.samples.map(s => Object.values(s.stages).flatMap(stage => stage.tasks)
    .filter(task => task.tables.includes('store_sales'))
    .sort((a, b) => b.output_eos_ms - a.output_eos_ms)[0]).filter(Boolean);
  group.latest_store_sales_task = {
    timing: averageObjects(criticalTasks.map(({ probes, stage, task, tables, stage_line, ...timing }) => timing)),
    probes: averageObjects(criticalTasks.map(task => task.probes)),
  };
  const representative = group.samples.reduce((a, b) => Math.abs(a.elapsed_ms - group.median_ms) <= Math.abs(b.elapsed_ms - group.median_ms) ? a : b);
  group.plan = `plans/${group.workers}w-${group.query}-${group.variant}.txt`;
  group.plan_source = representative.plan;
  group.representative_stages = representative.stages;
  fs.writeFileSync(path.join(root, group.plan), gunzipSync(fs.readFileSync(path.join(root, representative.plan))));
}
function pairedInterval(pairs) {
  let seed = 9262026;
  const rand = () => ((seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0) / 2 ** 32);
  const strata = [...new Set(pairs.map(p => p.session))].map(session => pairs.filter(p => p.session === session));
  const bootstrap = Array.from({ length: 10000 }, () => {
    const sampled = strata.flatMap(block => block.map(() => block[Math.floor(rand() * block.length)]));
    return mean(sampled.map(p => p.off_ms)) / mean(sampled.map(p => p.on_ms));
  }).sort((a, b) => a - b);
  return [bootstrap[249], bootstrap[9749]];
}
const comparisons = [];
for (const on of groups.values()) {
  if (on.variant !== 'A-on') continue;
  const off = groups.get(`${on.workers}/${on.query}/Control-on`);
  if (!off) continue;
  const pairs = on.samples.map(s => {
    const before = off.samples.find(o => o.session === s.session && o.pair === s.pair);
    return before ? { session: s.session, pair: s.pair, off_ms: before.elapsed_ms, on_ms: s.elapsed_ms,
      off_execution_ms: before.timings_ms.execution, on_execution_ms: s.timings_ms.execution } : null;
  }).filter(Boolean);
  comparisons.push({ workers: on.workers, query: on.query, paired_n: pairs.length,
    off_ms: off.mean_ms, on_ms: on.mean_ms, speedup: off.mean_ms / on.mean_ms,
    execution_speedup: off.mean_phases_ms.execution / on.mean_phases_ms.execution,
    paired_speedup_interval: pairedInterval(pairs),
    execution_speedup_interval: pairedInterval(pairs.map(p => ({ ...p,
      off_ms: p.off_execution_ms, on_ms: p.on_execution_ms }))), pairs });
}
const compactGroups = [...groups.values()].map(({ samples, ...rest }) => rest);
fs.writeFileSync(path.join(root, 'raw/parsed-samples.json.gz'), gzipSync(JSON.stringify([...groups.values()])));
fs.writeFileSync(path.join(root, 'summary.json'), JSON.stringify({ updated_at: new Date().toISOString(),
  status: manifest.status, measured: manifest.samples.filter(s => s.kind === 'measured').length,
  groups: compactGroups, comparisons }, null, 2) + '\n');
const lines = ['# Q80 / Q27 Scan Probe Measurements', '',
  `Run status: \`${manifest.status}\`; ${manifest.samples.filter(s => s.kind === 'measured').length} measured executions captured.`, '',
  'Parquet row pushdown and filter reordering are on in every entry.', '',
  '## Endpoint Latency', '',
  'Includes plan collection and rendering. This is not execution-only latency.', '',
  '| Workers | Query | N per case | Filters off | Filters on | Speedup | Paired 95% interval |',
  '| ---: | --- | ---: | ---: | ---: | ---: | --- |'];
for (const c of comparisons) lines.push(`| ${c.workers} | ${c.query} | ${c.paired_n} | ${fmt(c.off_ms)} ms | ${fmt(c.on_ms)} ms | ${fmt(c.speedup, 2)}x | ${c.paired_speedup_interval.map(x => fmt(x, 2)).join(' - ')}x |`);
lines.push('', 'Intervals resample off/on pairs within each fresh-pod session. They',
  'describe observed run variation, not uncertainty across all hardware or days.', '',
  '## Execution Latency', '',
  '| Query | DF off (ms) | DF on (ms) | Off/on speedup | Paired 95% interval |',
  '| --- | ---: | ---: | ---: | --- |');
for (const c of comparisons) {
  const off = groups.get(`${c.workers}/${c.query}/Control-on`);
  const on = groups.get(`${c.workers}/${c.query}/A-on`);
  lines.push(`| ${c.query} | ${fmt(off.mean_phases_ms.execution)} | ${fmt(on.mean_phases_ms.execution)} | ${fmt(c.execution_speedup, 3)}x | ${c.execution_speedup_interval.map(x => fmt(x, 3)).join(' - ')}x |`);
}
lines.push('',
  '## Timing Breakdown', '',
  '| Query / case | Planning | First result | Execution | Metrics | Rendering |',
  '| --- | ---: | ---: | ---: | ---: | ---: |');
for (const g of compactGroups) {
  const t = g.mean_phases_ms;
  lines.push(`| [${g.query} ${g.variant}](${g.plan}) | ${fmt(t.physical_planning)} | ${fmt(t.first_batch)} | ${fmt(t.execution)} | ${fmt(t.metrics_collection)} | ${fmt(t.plan_render)} |`);
}
lines.push('', 'All times are milliseconds. Planning is physical planning; initial SQL',
  'parsing and logical planning precede the endpoint timer. Execution includes',
  'waiting for output EOS and any work the output stream must join.', '',
  '## Session Means', '', '| Workers | Query / case | Session means (ms) |', '| ---: | --- | --- |');
for (const g of compactGroups) lines.push(`| ${g.workers} | ${g.query} ${g.variant} | ${Object.entries(g.sessions).map(([s, n]) => `S${s}: ${fmt(n)}`).join('; ')} |`);
lines.push('', 'Execution-only session means:', '',
  '| Query / case | Session 1 (ms) | Session 2 (ms) |', '| --- | ---: | ---: |');
for (const g of compactGroups) lines.push(`| ${g.query} ${g.variant} | ${fmt(g.session_phases_ms[1]?.execution)} | ${fmt(g.session_phases_ms[2]?.execution)} |`);
lines.push('', '## Plan Counters', '',
  'Counters average all measured plans. Durations below are summed work,',
  'not additive contributions to wall time. Bytes use decimal GB/MB.', '',
  '| Workers | Query / case | Scan rows (M) | Read GB | Network MB | Predicate ms | Apply ms |',
  '| ---: | --- | ---: | ---: | ---: | ---: | ---: |');
for (const g of compactGroups) {
  const m = g.mean_metrics;
  lines.push(`| ${g.workers} | ${g.query} ${g.variant} | ${fmt(m.scan_rows / 1e6, 2)} | ${fmt(m.scan_bytes / 1e9, 3)} | ${fmt(m.network_bytes / 1e6)} | ${fmt(m.row_predicate_ms)} | ${fmt(m.filter_apply_ms)} |`);
}
lines.push('', '## Scan CPU', '',
  'Times are aggregate milliseconds across tasks and files. Decode and row',
  'predicate CPU are nested within scan-poll CPU; do not add them together.', '',
  '| Query / case | Prepare CPU | Reader build CPU | Scan poll CPU | Decode CPU | Row filter CPU |',
  '| --- | ---: | ---: | ---: | ---: | ---: |');
for (const g of compactGroups) {
  const m = g.mean_metrics;
  lines.push(`| ${g.query} ${g.variant} | ${fmt(m.probe_prepare_filters_cpu)} | ${fmt(m.probe_build_reader_cpu)} | ${fmt(m.probe_scan_poll_cpu)} | ${fmt(m.probe_decode_cpu)} | ${fmt(m.probe_row_filter_cpu)} |`);
}
lines.push('', '## Decoder Data Requests', '',
  'Logical range-read calls, not HTTP requests. Summed await time includes',
  'overlapping I/O and scheduling waits. Metadata is excluded.', '',
  '| Query / case | Calls | Ranges | Bytes (GB) | Await ms | Poll CPU ms | Calls before first batch |',
  '| --- | ---: | ---: | ---: | ---: | ---: | ---: |');
for (const g of compactGroups) {
  const m = g.mean_metrics;
  lines.push(`| ${g.query} ${g.variant} | ${fmt(m.probe_range_read_calls, 0)} | ${fmt(m.probe_range_read_ranges, 0)} | ${fmt(m.probe_data_bytes_requested / 1e9, 3)} | ${fmt(m.probe_range_read_wall)} | ${fmt(m.probe_range_read_cpu)} | ${fmt(m.probe_first_batch_read_calls, 0)} |`);
}
lines.push('', '## Repartition Coalescing', '',
  'EOF means the last input sender finished. Delay is first input to first',
  'coalesced output, averaged over nonempty output buckets.', '',
  '| Query / case | Regular rows (M) | EOF rows (M) | Rows at EOF | Buckets first emitting at EOF | First emit delay (ms) |',
  '| --- | ---: | ---: | ---: | ---: | ---: |');
for (const g of compactGroups) {
  const m = g.mean_metrics;
  const regular = m.probe_coalesce_regular_rows, eof = m.probe_coalesce_eof_rows;
  lines.push(`| ${g.query} ${g.variant} | ${fmt(regular / 1e6, 2)} | ${fmt(eof / 1e6, 2)} | ${fmt(100 * eof / (regular + eof))}% | ${fmt(100 * m.probe_coalesce_first_emit_at_eof / m.probe_coalesce_output_buckets)}% | ${fmt(m.probe_coalesce_first_emit_wall / m.probe_coalesce_output_buckets)} |`);
}
lines.push('', '## Store Sales Stage Timing', '',
  'First output is a task mean; finish is the slowest task. Timestamps are',
  'relative to distributed query start. Coalescer delay starts at first input,',
  'not query start. These aggregates must not be added as a latency breakdown.', '',
  '| Query / case | Stage | First output | Finish | First emitting at EOF | Coalescer delay |',
  '| --- | --- | ---: | ---: | ---: | ---: |');
for (const g of compactGroups) {
  for (const [name, stage] of Object.entries(g.stages).filter(([, s]) => s.tables.includes('store_sales'))) {
    const m = stage.probes;
    lines.push(`| ${g.query} ${g.variant} | ${name} | ${fmt(stage.timing.first_output_mean_ms)} | ${fmt(stage.timing.output_eos_max_ms)} | ${fmt(100 * m.probe_coalesce_first_emit_at_eof / m.probe_coalesce_output_buckets)}% | ${fmt(m.probe_coalesce_first_emit_wall / m.probe_coalesce_output_buckets)} |`);
  }
}
lines.push('', 'See [summary.json](summary.json) for per-table and per-stage metrics,',
  'representative task timings, phase means, and every paired latency.', '');
fs.writeFileSync(path.join(root, 'measurements.md'), lines.join('\n'));
console.log(lines.slice(0, 11).join('\n'));
