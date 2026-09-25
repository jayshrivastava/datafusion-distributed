import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import os from 'node:os';

const root = path.dirname(fileURLToPath(import.meta.url));
const data = '/instance_storage/bits_cache/datafusion-benchmark-data/tpch/sf10';
const reference = JSON.parse(fs.readFileSync(path.join(root,
  '../tpch-sf10-post-scan-filters/samples.json')));
const expected = new Map(reference.map(s => [s.query, JSON.stringify(s.rows)]));
const source = '/home/bits/datafusion-distributed';
const git = (...args) => {
  const result = spawnSync('git', args, { cwd: source, encoding: 'utf8' });
  if (result.status !== 0) throw Error(result.stderr);
  return result.stdout;
};
fs.writeFileSync(path.join(root, 'tracked-changes.patch'), git('diff'));
fs.mkdirSync(path.join(root, 'source'), { recursive: true });
for (const file of [
  'src/coordinator/partitioned_dynamic_filter.rs',
  'src/coordinator/dynamic_filter_registry.rs',
  'src/coordinator/query_coordinator.rs',
  'src/distributed_planner/insert_post_scan_dynamic_filters.rs',
  'src/execution_plans/task_variants.rs',
  'benchmarks/src/bin/tpch-filter-matrix.rs',
]) fs.copyFileSync(path.join(source, file), path.join(root, 'source', path.basename(file)));
const digest = binary => createHash('sha256')
  .update(fs.readFileSync(path.join(root, `${binary}-runner`))).digest('hex');
fs.writeFileSync(path.join(root, 'metadata.json'), JSON.stringify({
  started_at: new Date().toISOString(), head: git('rev-parse', 'HEAD').trim(),
  branch: git('branch', '--show-current').trim(), status: git('status', '--short'),
  arch: os.arch(), cpus: os.cpus().length, memory_bytes: os.totalmem(), data,
  baseline_sha256: digest('baseline'), candidate_sha256: digest('candidate'),
  order: ['baseline-1', 'candidate-1', 'candidate-2', 'baseline-2'],
  workers: 4, partitions: 4, iterations_per_block: 5,
}, null, 2) + '\n');
for (const [binary, block] of [
  ['baseline', 'baseline-1'], ['candidate', 'candidate-1'],
  ['candidate', 'candidate-2'], ['baseline', 'baseline-2'],
]) {
  const output = path.join(root, block);
  fs.mkdirSync(output);
  console.log(`${new Date().toISOString()} START ${block}`);
  const log = fs.openSync(path.join(output, 'run.log'), 'w');
  const result = spawnSync(path.join(root, `${binary}-runner`), [
    '--data', data, '--output', output, '--iterations', '5',
  ], { stdio: ['ignore', log, log] });
  fs.closeSync(log);
  if (result.error || result.status !== 0) {
    throw result.error ?? Error(`${block} failed: ${result.status}; see run.log`);
  }
  const samples = JSON.parse(fs.readFileSync(path.join(output, 'samples.json')));
  if (samples.length !== 72) throw Error(`${block}: expected 72 executions`);
  for (const s of samples) {
    if (JSON.stringify(s.rows) !== expected.get(s.query)) {
      throw Error(`${block} ${s.query} ${s.case.name}: baseline result mismatch`);
    }
  }
  console.log(`${new Date().toISOString()} PASS ${block}: 72 results match`);
}
