import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { stripVTControlCharacters } from 'node:util';
import { gunzipSync, gzipSync } from 'node:zlib';
import { DataFusionRunner, dataFusionSettingStatements } from '../../benchmarks-remote/src/bin/datafusion-bench.ts';
import { withKubectlPortForward } from '../../benchmarks-remote/src/lib/port-forward.ts';
import { BenchmarkRun, BenchResult } from '../../benchmarks-remote/src/lib/results.ts';

const tools = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const repo = path.resolve(tools, '../datafusion-distributed');
const root = path.join(repo, 'reports/remote-tpcds-q80-q27-scan-probes');
const prior = path.join(repo, 'reports/remote-tpcds-dynamic-filters');
const manifestFile = path.join(root, 'manifest.json');
const kubeconfig = path.join(tools, 'benchmarks-remote/k8s/.kubeconfig');
const source = '/instance_storage/bits_cache/remote-tpcds-q80-q27-scan-probes/source-A';
const binary = '/instance_storage/bits_cache/datafusion-distributed-target/x86_64-unknown-linux-gnu/release/worker';
const [action, value] = process.argv.slice(2);
assert(['init', 'deploy', 'smoke', 'run', 'verify', 'finish'].includes(action));

function writeJson(file: string, data: unknown) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(`${file}.tmp`, JSON.stringify(data, null, 2) + '\n');
  fs.renameSync(`${file}.tmp`, file);
}
const sha256 = (data: Buffer | string) => createHash('sha256').update(data).digest('hex');
function save() {
  manifest.updated_at = new Date().toISOString();
  writeJson(manifestFile, manifest);
}
let manifest: any;
if (action === 'init') {
  const previous = JSON.parse(fs.readFileSync(path.join(prior, 'manifest.json'), 'utf8'));
  assert(!fs.existsSync(manifestFile), 'Do not overwrite an existing run');
  assert.equal(execFileSync('git', ['rev-parse', 'HEAD'], { cwd: source, encoding: 'utf8' }).trim(), previous.commit);
  const patch = execFileSync('git', ['diff', '--binary'], { cwd: source, maxBuffer: 8 * 1024 * 1024 });
  fs.writeFileSync(path.join(root, 'instrumentation.patch'), patch, { flag: 'wx' });
  const artifactHash = sha256(fs.readFileSync(binary));
  const artifactPath = path.resolve(root, 'raw/worker-A-instrumented');
  fs.copyFileSync(binary, artifactPath, fs.constants.COPYFILE_EXCL);
  const dependencyProvenance = JSON.parse(fs.readFileSync(path.join(root, 'dependency-provenance.json'), 'utf8'));
  assert.equal(dependencyProvenance.lockfile_sha256, sha256(fs.readFileSync(path.join(source, 'Cargo.lock'))));
  fs.mkdirSync(path.join(root, 'queries'), { recursive: true });
  const inputs = Object.fromEntries(['q80', 'q27'].map(query => {
    const sql = fs.readFileSync(path.join(prior, 'queries', `${query}.sql`));
    fs.writeFileSync(path.join(root, 'queries', `${query}.sql`), sql, { flag: 'wx' });
    const expectedRows = previous.runs.find((run: any) => run.dataset === 'tpcds/sf10' && run.query === query).samples[0].row_count;
    return [query, { sql: `queries/${query}.sql`, sha256: sha256(sql), expected_rows: expectedRows }];
  }));
  fs.copyFileSync(path.join(prior, 'raw/datasets/tpcds-sf10-tables.json'),
    path.join(root, 'raw/tpcds-sf10-tables.json'), fs.constants.COPYFILE_EXCL);
  manifest = {
    created_at: new Date().toISOString(), status: 'prepared', source_commit: previous.commit,
    build: { profile: 'release', target: 'x86_64-unknown-linux-gnu',
      rustc: execFileSync('rustc', ['--version'], { cwd: source, encoding: 'utf8' }).trim(),
      command: 'cargo zigbuild --locked --manifest-path benchmarks/remote-worker/Cargo.toml --package datafusion-distributed-remote-worker --release --bin worker --target x86_64-unknown-linux-gnu' },
    source_patch_sha256: sha256(patch), lockfile_sha256: sha256(fs.readFileSync(path.join(source, 'Cargo.lock'))),
    dependency_provenance: dependencyProvenance,
    tools_commit: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: tools, encoding: 'utf8' }).trim(),
    foundation: previous.foundation, deployment: previous.deployment,
    workers: { instance_type: previous.workers.instance_type, cpu: previous.workers.cpu, memory: previous.workers.memory },
    artifact: { path: artifactPath, sha256: artifactHash,
      uri: `s3://${previous.foundation.datasetBucketName}/.benchmark-artifacts/datafusion/${artifactHash}/worker` },
    dataset: 'tpcds/sf10', queries: ['q80', 'q27'],
    query_inputs: inputs,
    experiment: 'A only; dynamic filters off/on, Parquet row pushdown and reorder on in both cases; scan CPU/read/coalescer probes',
    sessions: [
      { number: 1, workers: 12, partitions: 15, queries: ['q80', 'q27'] },
      { number: 2, workers: 12, partitions: 15, queries: ['q27', 'q80'] },
    ], pairs_per_session: 10, samples: [], errors: [],
  };
  save();
  console.log(JSON.stringify({ artifact: manifest.artifact, source: manifest.source_commit }, null, 2));
  process.exit(0);
}
manifest = JSON.parse(fs.readFileSync(manifestFile, 'utf8'));
assert(process.env.AWS_PROFILE, 'Set the caller-selected AWS_PROFILE');
const env = { ...process.env, AWS_REGION: manifest.foundation.region };
const deployment = `${manifest.deployment}-worker`;
const namespace = 'benchmark-datafusion';
const kargs = ['--kubeconfig', kubeconfig, '--context', manifest.foundation.clusterName, '--request-timeout=60s'];
const session = manifest.sessions.find((s: any) => s.number === Number(value));
assert(session, 'Specify session 1..2');
function kubectl(args: string[]) {
  return JSON.parse(execFileSync('kubectl', [...kargs, ...args, '-o', 'json'],
    { env, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024 }));
}
function unique(label: string, extension: string, directory = 'logs') {
  return path.join(root, 'raw', directory,
    `${label}-${new Date().toISOString().replaceAll(':', '-')}.${extension}`);
}
function snapshot(label: string, validate = true, expectedIds?: string[]) {
  const pods = kubectl(['get', 'pods', '-A']);
  const nodes = kubectl(['get', 'nodes']);
  const active = pods.items.filter((p: any) => /^benchmark-(datafusion|trino|spark|ballista)$/.test(p.metadata.namespace)
    && ['Running', 'Pending'].includes(p.status.phase));
  const ours = active.filter((p: any) => p.metadata.namespace === namespace
    && p.metadata.labels?.['app.kubernetes.io/name'] === deployment);
  const other = active.filter((p: any) => !ours.includes(p));
  const ownNodes = new Set(ours.map((p: any) => p.spec.nodeName).filter(Boolean));
  const file = unique(label, 'json', 'deployments');
  writeJson(file, { at: new Date().toISOString(), pods, nodes });
  assert(other.every((p: any) => !p.spec.nodeName || !ownNodes.has(p.spec.nodeName)),
    'Another benchmark shares a measured node');
  if (validate) {
    assert.equal(ours.length, session.workers);
    assert.equal(ownNodes.size, session.workers);
    for (const pod of ours) {
      assert(!pod.metadata.deletionTimestamp && pod.status.phase === 'Running');
      assert(pod.status.containerStatuses.every((c: any) => c.ready && c.restartCount === 0), 'Worker restarted or unready');
      assert(pod.spec.initContainers.some((c: any) => c.args?.some((s: string) => s.includes(manifest.artifact.uri))),
        'Wrong worker artifact');
      const worker = pod.spec.containers.find((c: any) => c.name === 'worker');
      for (const resources of [worker.resources.requests, worker.resources.limits]) {
        assert.equal(resources.cpu, manifest.workers.cpu);
        assert.equal(resources.memory, manifest.workers.memory);
      }
      const node = nodes.items.find((n: any) => n.metadata.name === pod.spec.nodeName);
      assert.equal(node.metadata.labels['node.kubernetes.io/instance-type'], manifest.workers.instance_type);
    }
  }
  const ids = ours.map((p: any) => p.metadata.uid).sort();
  if (expectedIds) assert.deepEqual(ids, expectedIds, 'Worker pods changed within the session');
  return { ids, file: path.relative(root, file) };
}
async function command(label: string, executable: string, args: string[], extra = {}) {
  const log = unique(label, 'log');
  fs.writeFileSync(log, `$ ${JSON.stringify([executable, ...args])}\n`, { flag: 'wx' });
  await new Promise<void>((resolve, reject) => {
    const child = spawn(executable, args, { cwd: tools, env: { ...env, ...extra }, stdio: ['ignore', 'pipe', 'pipe'] });
    for (const stream of [child.stdout, child.stderr]) stream.on('data', chunk => {
      fs.appendFileSync(log, chunk); process.stdout.write(chunk);
    });
    child.once('error', reject);
    child.once('close', code => code === 0 ? resolve() : reject(new Error(`${label} exited ${code}; see ${log}`)));
  });
}
function forward<T>(callback: () => Promise<T>) {
  return withKubectlPortForward({ clusterName: manifest.foundation.clusterName, deployment: 'datafusion',
    service: manifest.deployment, region: manifest.foundation.region, kubeconfig }, callback);
}
async function info() {
  for (let attempt = 0; attempt < 60; attempt++) {
    const response = await fetch('http://localhost:9000/info', { signal: AbortSignal.timeout(30_000) });
    assert(response.ok);
    const result = await response.json() as any;
    assert.equal(result.git_commit_hash, manifest.source_commit);
    if (result.worker_urls.length === session.workers && result.errors.length === 0) {
      writeJson(unique(`s${session.number}-info`, 'json', 'deployments'), result);
      return;
    }
    await delay(2000);
  }
  throw new Error('Worker discovery did not converge');
}
function runner(dynamic: boolean, label: string) {
  return new DataFusionRunner({
    resultName: label, dataset: manifest.dataset, queries: 'q80,q27', iterations: 1,
    timeSecs: 0, warmup: false, debug: false, compare: false,
    url: 'http://localhost:9000', bucket: `s3://${manifest.foundation.datasetBucketName}`,
    clusterName: manifest.foundation.clusterName, region: manifest.foundation.region,
    service: manifest.deployment, testdataRoot: path.join(root, 'raw/results'), kubeconfig,
    targetPartitions: session.partitions,
    fileScanConfigBytesPerPartition: undefined, cardinalityTaskSf: undefined,
    batchSize: undefined, shuffleBatchSize: undefined, childrenIsolatorUnions: undefined,
    broadcastJoins: undefined, partialReduce: undefined, collectMetrics: undefined,
    compression: undefined, maxTasksPerStage: undefined, repartitionFileMinSize: undefined,
    dynamic: undefined, dynamicBytesPerPartition: undefined,
    configs: [
      `datafusion.optimizer.enable_dynamic_filter_pushdown=${dynamic}`,
      'datafusion.execution.parquet.pushdown_filters=true',
      'datafusion.execution.parquet.reorder_filters=true',
    ],
  });
}
async function configure(r: DataFusionRunner, dynamic: boolean, label: string, check: boolean) {
  // Providers retain creation-time options, so set options before registering tables.
  await r.executeQuery(dataFusionSettingStatements(r.options));
  if (check) {
    const expected = {
      'datafusion.optimizer.enable_dynamic_filter_pushdown': String(dynamic),
      'datafusion.execution.parquet.pushdown_filters': 'true',
      'datafusion.execution.parquet.reorder_filters': 'true',
      'datafusion.execution.target_partitions': String(session.partitions),
      'datafusion.execution.batch_size': '8192',
      'datafusion.execution.parquet.pruning': 'true',
      'distributed.compression': 'lz4', 'distributed.broadcast_joins': 'true',
      'distributed.shuffle_batch_size': '0',
      'distributed.children_isolator_unions': 'true', 'distributed.dynamic_task_count': 'false',
      'distributed.partial_reduce': 'false', 'distributed.max_tasks_per_stage': '0',
      'distributed.collect_metrics': 'true', 'distributed.collect_dynamic_filters': 'true',
    };
    await r.executeQuery('SET datafusion.catalog.information_schema = true');
    try {
      const predicate = Object.entries(expected).map(([name, v]) =>
        `(name='${name.replace(/^distributed\./, '')}' AND value='${v}')`).join(' OR ');
      const result = await r.executeQuery(`SELECT name, value FROM information_schema.df_settings WHERE ${predicate}`);
      assert.equal(result.rowCount, Object.keys(expected).length, 'Unexpected session settings');
      writeJson(unique(`${label}-settings`, 'json', 'deployments'), { expected, result });
    } finally {
      await r.executeQuery('SET datafusion.catalog.information_schema = false');
    }
  }
  const tables = JSON.parse(fs.readFileSync(path.join(root, 'raw/tpcds-sf10-tables.json'), 'utf8'));
  assert.equal(tables.length, 24);
  assert(tables.every((t: any) => t.fileType === 'PARQUET' && t.s3Path.startsWith(r.options.bucket + '/tpcds/sf10/')));
  await r.createTables(tables);
}
async function sample(query: string, dynamic: boolean, pair: number, kind: string, check: boolean) {
  const variant = dynamic ? 'A-on' : 'Control-on';
  const label = `s${session.number}-${query}-${variant}`;
  const planPath = `raw/plans/${label}-${kind}-${pair}.txt.gz`;
  assert(!fs.existsSync(path.join(root, planPath)), `Sample already exists: ${planPath}`);
  const r = runner(dynamic, label);
  await configure(r, dynamic, label, check);
  const input = manifest.query_inputs[query];
  const sql = fs.readFileSync(path.join(root, input.sql), 'utf8');
  assert.equal(sha256(sql), input.sha256);
  const result = await r.executeQuery(sql);
  const plan = stripVTControlCharacters(result.plan);
  fs.writeFileSync(path.join(root, planPath), gzipSync(plan + '\n'), { flag: 'wx' });
  assert.equal(result.rowCount, input.expected_rows, 'Result row count changed');
  assert(Number.isFinite(result.elapsed) && result.elapsed > 0);
  for (const name of ['BenchmarkTimings:', 'output_stream_finished_at', 'output_streams_completed',
    'dynamic_filter_register_time', 'dynamic_filter_dispatch_time', 'dynamic_filter_apply_time',
    'probe_prepare_filters_cpu', 'probe_build_reader_cpu', 'probe_scan_poll_cpu',
    'probe_row_filter_cpu', 'probe_decode_cpu', 'probe_range_read_calls',
    'probe_range_read_wall', 'probe_first_batch_read_calls', 'probe_coalesce_eof_rows',
    'probe_coalesce_first_emit_wall', 'probe_coalesce_first_emit_at_eof']) {
    assert(plan.includes(name), `Missing instrumentation ${name}`);
  }
  if (!dynamic) assert(!plan.includes('DynamicFilter ['), 'Disabled query contains a dynamic predicate');
  for (const name of ['probe_prepare_filters_cpu', 'probe_build_reader_cpu', 'probe_decode_cpu',
    'probe_scan_poll_cpu', 'probe_range_read_calls', 'probe_data_bytes_requested']) {
    const values = [...plan.matchAll(new RegExp(`${name}=(\\{[^}]*\\}|[^,\\]\\n]+)`, 'g'))]
      .flatMap(match => match[1].replace(/[{}]/g, '').split(','))
      .map(value => Number.parseFloat(value.includes(':') ? value.split(':')[1] : value));
    assert(values.some(value => value > 0), `Probe never recorded work: ${name}`);
  }
  const applied = [...plan.matchAll(/dynamic_filter_updates_applied=(\{[^}]*\}|[^,\]\n]+)/g)]
    .flatMap(match => match[1].replace(/[{}]/g, '').split(','))
    .map(value => Number.parseFloat(value.includes(':') ? value.split(':')[1] : value))
    .reduce((sum, value) => sum + value, 0);
  assert(dynamic ? applied > 0 : applied === 0, 'Unexpected remote-filter application count');
  assert(!plan.includes('dynamic_filter_global_hash_merges'), 'Finer-hash B build is not part of this experiment');
  const timings = Object.fromEntries([...plan.split('\n')[0].matchAll(/(\w+)=([\d.]+)ms/g)]
    .map(match => [match[1], Number(match[2])]));
  assert(['physical_planning', 'execution', 'first_batch', 'metrics_collection', 'plan_render']
    .every(key => Number.isFinite(timings[key])), 'Missing phase timings');
  const entry = { session: session.number, workers: session.workers, partitions: session.partitions,
    query, variant, pair, kind, elapsed_ms: result.elapsed, row_count: result.rowCount,
    tasks: result.tasks, timings_ms: timings, plan: planPath, at: new Date().toISOString() };
  manifest.samples.push(entry);
  save();
  if (kind === 'measured') {
    const native = new BenchResult(manifest.dataset, label, query, r.options.testdataRoot);
    const nativeRun = new BenchmarkRun(manifest.dataset, label, undefined, r.options.testdataRoot);
    for (const sample of manifest.samples.filter((s: any) => s.session === session.number && s.query === query
      && s.variant === variant && s.kind === 'measured')) {
      native.iterations.push({ elapsed: sample.elapsed_ms, rowCount: sample.row_count,
        tasks: sample.tasks, plan: gunzipSync(fs.readFileSync(path.join(root, sample.plan))).toString('utf8') });
    }
    nativeRun.results.push(native);
    nativeRun.store();
  }
  console.log(`${label} ${kind} ${pair}: ${result.elapsed.toFixed(1)} ms, execution=${timings.execution} ms`);
}

try {
  execFileSync('aws', ['sts', 'get-caller-identity'], { env, stdio: 'ignore' });
  if (action === 'deploy') {
    const before = snapshot(`s${session.number}-before-deploy`, false);
    assert.equal(sha256(fs.readFileSync(manifest.artifact.path)), manifest.artifact.sha256);
    await command(`s${session.number}-deploy`, 'npm', ['run', 'datafusion-deploy'], {
      DEPLOYMENT_NAME: manifest.deployment, WORKER_ARTIFACT: manifest.artifact.uri,
      NODE_COUNT: String(session.workers), BENCHMARK_INSTANCE_TYPE: manifest.workers.instance_type,
      BENCHMARK_WORKER_CPU: manifest.workers.cpu, BENCHMARK_WORKER_MEMORY: manifest.workers.memory,
    });
    // A replica-count update can return while removed pods are still terminating.
    // Restart retained pods before requiring the final, fresh-pod worker count.
    let after = snapshot(`s${session.number}-after-deploy`, false);
    if (after.ids.some(id => before.ids.includes(id))) {
      await command(`s${session.number}-restart`, 'kubectl', [...kargs, 'rollout', 'restart', `deployment/${deployment}`, '-n', namespace]);
      await command(`s${session.number}-ready`, 'kubectl', [...kargs, 'rollout', 'status', `deployment/${deployment}`, '-n', namespace, '--timeout=15m']);
    }
    after = snapshot(`s${session.number}-fresh-deploy`);
    assert(after.ids.every(id => !before.ids.includes(id)), 'Expected fresh worker pods');
    await forward(info);
    session.pods = after.ids;
    manifest.current_session = session.number;
    manifest.status = `deployed_session_${session.number}`;
    save();
  } else if (action === 'finish') {
    assert.equal(session.workers, 12);
    assert.equal(manifest.samples.filter((s: any) => s.kind === 'measured').length, 80);
    snapshot('finish-before', true, session.pods);
    await command('restore-defaults', 'kubectl', [...kargs, 'rollout', 'restart', `deployment/${deployment}`, '-n', namespace]);
    await command('restore-ready', 'kubectl', [...kargs, 'rollout', 'status', `deployment/${deployment}`, '-n', namespace, '--timeout=15m']);
    const after = snapshot('restored-defaults');
    assert(after.ids.every(id => !session.pods.includes(id)), 'Expected fresh sessions after cleanup');
    await forward(info);
    manifest.final_deployment = { workers: session.workers, pods: after.ids, snapshot: after.file,
      at: new Date().toISOString(), session_settings: 'Fresh worker defaults; temporary SQL settings cleared' };
    manifest.status = 'complete';
    save();
  } else {
    assert.equal(manifest.current_session, session.number);
    const before = snapshot(`s${session.number}-${action}-before`, true, session.pods);
    await forward(async () => {
      await info();
      if (action === 'verify') return;
      if (action === 'smoke') {
        const inventory = JSON.parse(execFileSync('aws', ['s3api', 'list-objects-v2',
          '--bucket', manifest.foundation.datasetBucketName, '--prefix', 'tpcds/sf10/',
          '--no-paginate', '--output', 'json'], { env, encoding: 'utf8', maxBuffer: 4 * 1024 * 1024 }));
        assert(!inventory.IsTruncated, 'Dataset inventory is incomplete');
        const files = inventory.Contents.filter((object: any) => object.Key.endsWith('.parquet'));
        assert.equal(files.length, 384, 'Expected the existing 24-table, 16-file dataset');
        writeJson(path.join(root, 'raw/tpcds-sf10-objects.json'), inventory);
        manifest.dataset_snapshot = { files: files.length,
          bytes: files.reduce((sum: number, object: any) => sum + object.Size, 0),
          sha256: sha256(JSON.stringify(files)), captured_at: new Date().toISOString() };
        save();
        await sample('q80', false, 0, 'smoke', true);
        await sample('q80', true, 0, 'smoke', true);
        return;
      }
      manifest.status = `running_session_${session.number}`;
      save();
      for (const query of session.queries) {
        if (manifest.samples.filter((s: any) => s.session === session.number && s.query === query && s.kind === 'measured').length === 20) continue;
        assert(!manifest.samples.some((s: any) => s.session === session.number && s.query === query && s.kind === 'measured'),
          'Partial query block: investigate before resuming');
        await sample(query, false, 0, 'warmup', true);
        await sample(query, true, 0, 'warmup', true);
        for (let pair = 1; pair <= manifest.pairs_per_session; pair++) {
          const order = (pair + session.number) % 2 === 0 ? [false, true] : [true, false];
          for (const dynamic of order) await sample(query, dynamic, pair, 'measured', false);
        }
        snapshot(`s${session.number}-${query}-after`, true, before.ids);
      }
    });
    snapshot(`s${session.number}-${action}-after`, true, before.ids);
    manifest.status = action === 'run' ? `session_${session.number}_complete` : `${action}_complete`;
    save();
  }
} catch (error) {
  manifest.status = 'stopped_after_error';
  manifest.errors.push({ at: new Date().toISOString(), action, session: session.number, error: String(error) });
  save();
  throw error;
}
