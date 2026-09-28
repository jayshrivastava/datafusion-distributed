import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { gunzipSync } from 'node:zlib';

const tools = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const root = path.resolve(tools, '../datafusion-distributed/reports/remote-tpcds-q80-q27-scan-probes');
const manifest = JSON.parse(fs.readFileSync(path.join(root, 'manifest.json')));
const groups = JSON.parse(gunzipSync(fs.readFileSync(path.join(root, 'raw/parsed-samples.json.gz'))));
const hash = file => createHash('sha256').update(fs.readFileSync(file)).digest('hex');
assert.equal(manifest.status, 'complete');
assert.equal(manifest.errors.length, 0);
assert.equal(manifest.samples.filter(s => s.kind === 'measured').length, 80);
assert.equal(manifest.samples.filter(s => s.kind === 'warmup').length, 8);
assert.equal(manifest.samples.filter(s => s.kind === 'smoke').length, 2);
assert.equal(manifest.final_deployment.workers, 12);
assert.equal(manifest.final_deployment.pods.length, 12);
assert(manifest.sessions[0].pods.every(id => !manifest.sessions[1].pods.includes(id)));
assert(manifest.final_deployment.pods.every(id => !manifest.sessions[1].pods.includes(id)));
assert.equal(hash(manifest.artifact.path), manifest.artifact.sha256);
assert.equal(hash(path.join(root, 'instrumentation.patch')), manifest.source_patch_sha256);
for (const dependency of Object.values(manifest.dependency_provenance.dependencies)) {
  assert.equal(hash(path.join(root, dependency.patch)), dependency.patch_sha256);
}
assert.equal(groups.length, 4);
for (const group of groups) {
  assert.equal(group.samples.length, 20);
  for (const session of [1, 2]) assert.equal(group.samples.filter(s => s.session === session).length, 10);
  const enabled = group.variant === 'A-on';
  for (const sample of group.samples) {
    assert.equal(sample.row_count, 100);
    assert.equal(sample.totals.streams_started, sample.totals.streams_completed);
    assert.equal(sample.totals.filter_updates_applied > 0, enabled);
    const table = sample.tables.store_sales;
    assert.equal(table.probe_range_read_calls, table.probe_scan_output_streams * (enabled ? 5 : 1));
    assert.equal(table.probe_first_batch_read_calls, table.probe_range_read_calls);
    assert(table.probe_scan_poll_cpu > 0);
    assert.equal(table.probe_data_bytes_requested, group.query === 'q80' ? 1057880000 : 4495500000);
    for (const stage of Object.values(sample.stages).filter(s => s.tables.includes('store_sales'))) {
      const m = stage.probes;
      assert.equal(m.probe_coalesce_first_emit_at_eof, enabled ? m.probe_coalesce_output_buckets : 0);
      assert.equal(m.probe_coalesce_regular_rows === 0, enabled);
    }
  }
}
for (const file of ['report.md', 'measurements.md', 'plan.md']) {
  const lines = fs.readFileSync(path.join(root, file), 'utf8').split('\n');
  let tableColumns = null;
  for (const line of lines) {
    if (line.startsWith('|')) {
      const columns = line.split('|').length - 2;
      if (tableColumns === null) tableColumns = columns;
      assert.equal(columns, tableColumns, `${file}: malformed table row`);
    } else tableColumns = null;
    assert(line.length <= 150, `${file}: overlong line (${line.length})`);
  }
}
const result = { checked_at: new Date().toISOString(), measured: 80, warmups: 8, smoke: 2,
  groups: 4, cases_per_group: 20, query_errors: 0, final_workers: 12,
  checks: ['artifact and patch hashes', 'sample coverage', 'expected row counts',
    'output stream completion', 'remote applications only when enabled',
    'five read phases with unchanged fact data bytes in all enabled samples',
    'fact shuffle output only at EOF in all enabled samples',
    'fresh pod sessions and cleanup', 'Markdown tables and line lengths'] };
fs.writeFileSync(path.join(root, 'audit.json'), JSON.stringify(result, null, 2) + '\n');
console.log(JSON.stringify(result, null, 2));
