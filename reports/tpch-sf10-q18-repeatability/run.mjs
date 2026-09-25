import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(fileURLToPath(import.meta.url));
const source = path.resolve(root, '../..');
const data = '/instance_storage/bits_cache/datafusion-benchmark-data/tpch/sf10';
const binary = path.join(root, 'runner');
const manifestPath = path.join(root, 'manifest.json');
assert(!fs.existsSync(manifestPath), 'Keep previous runs; use a new output directory');
const reference = JSON.parse(fs.readFileSync(path.join(source,
  'reports/tpch-sf10-local-filter-matrix/samples.json')))
  .find(s => s.query === 'q18' && s.case.name === 'all_off').rows;
const hash = file => createHash('sha256').update(fs.readFileSync(file)).digest('hex');
fs.copyFileSync(path.join(source, 'target/release/tpch-filter-matrix'), binary);
const files = [
  'Cargo.lock', 'benchmarks/src/bin/tpch-filter-matrix.rs',
  'src/coordinator/partitioned_dynamic_filter.rs',
  'src/coordinator/dynamic_filter_registry.rs',
  'src/coordinator/query_coordinator.rs',
  'src/distributed_planner/insert_post_scan_dynamic_filters.rs',
];
for (const file of files) {
  const destination = path.join(root, 'source', file);
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.copyFileSync(path.join(source, file), destination);
}
fs.writeFileSync(path.join(root, 'tracked-changes.patch'),
  execFileSync('git', ['diff'], { cwd: source }));
const manifest = {
  status: 'starting', started_at: new Date().toISOString(),
  commit: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: source, encoding: 'utf8' }).trim(),
  binary_sha256: hash(binary), source_sha256: Object.fromEntries(files.map(f => [f, hash(path.join(source, f))])),
  data, query: 'q18', workers: 4, target_partitions: 4, iterations: 20,
  arch: os.arch(), cpu: os.cpus()[0].model, cpus: os.cpus().length,
  memory_bytes: os.totalmem(), rounds: [],
};
const save = () => fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + '\n');
const metric = (s, key) => s.metrics.reduce((n, m) => n + (m.values[key] ?? 0), 0);
try {
  for (let round = 1; round <= 3; round++) {
    const directory = `round-${round}`;
    const output = path.join(root, directory);
    fs.mkdirSync(output);
    const args = ['--data', data, '--output', output, '--queries', 'q18',
      '--paired-dynamic-filtering', '--iterations', '20'];
    if (round === 2) args.push('--reverse-order');
    const entry = { directory, args, started_at: new Date().toISOString(), load_before: os.loadavg() };
    manifest.status = `running_${directory}`;
    manifest.rounds.push(entry); save();
    console.log(`START ${directory}`);
    const log = path.join(output, 'run.log');
    await new Promise((resolve, reject) => {
      const child = spawn(binary, args, { cwd: source, stdio: ['ignore', 'pipe', 'pipe'] });
      for (const stream of [child.stdout, child.stderr]) stream.on('data', chunk => {
        fs.appendFileSync(log, chunk); process.stdout.write(chunk);
      });
      child.on('error', reject);
      child.on('close', (code, signal) => code === 0 ? resolve()
        : reject(new Error(`${directory} exited ${code ?? signal}`)));
    });
    const samples = JSON.parse(fs.readFileSync(path.join(output, 'samples.json')));
    assert.equal(samples.length, 42);
    for (const sample of samples) {
      assert.equal(sample.query, 'q18');
      assert.deepEqual(sample.rows, reference, 'Result differs from saved reference');
      assert(sample.case.pushdown_filters && sample.case.reorder_filters);
      const plan = fs.readFileSync(path.join(output, sample.plan), 'utf8');
      assert(plan.includes('metrics=['));
      assert(!sample.metrics.some(m => m.post_scan_filter && m.dynamic_filter),
        'Unexpected dynamic filter directly above a scan');
      if (sample.case.dynamic_filtering) {
        assert(metric(sample, 'dynamic_filter_updates_received') > 0);
        assert(metric(sample, 'dynamic_filter_global_hash_merges') > 0);
      } else {
        assert.equal(metric(sample, 'dynamic_filter_updates_received'), 0);
        assert(!plan.includes('DynamicFilter ['));
      }
    }
    Object.assign(entry, { finished_at: new Date().toISOString(), load_after: os.loadavg(),
      validated: true, executions: samples.length });
    save();
    console.log(`PASS ${directory}: all 42 results match, plans and configuration validated`);
  }
  manifest.status = 'complete';
  manifest.finished_at = new Date().toISOString();
  save();
} catch (error) {
  manifest.status = 'stopped_after_error'; manifest.error = String(error); save();
  throw error;
}
