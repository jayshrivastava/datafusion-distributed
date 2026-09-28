import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const root = '/instance_storage/bits_cache/remote-tpcds-q80-q27-scan-probes';
const cwd = path.join(root, 'source-A');
const env = { ...process.env };
for (const key of Object.keys(env)) {
  if (/^(AWS_|GH_|GITHUB_)/.test(key)) delete env[key];
}
Object.assign(env, {
  AWS_CONFIG_FILE: '/dev/null', AWS_SHARED_CREDENTIALS_FILE: '/dev/null',
  AWS_EC2_METADATA_DISABLED: 'true', CARGO_BUILD_JOBS: '4',
  CARGO_TARGET_DIR: '/instance_storage/bits_cache/datafusion-distributed-target',
  CARGO_PROFILE_DEV_DEBUG: '0', CARGO_PROFILE_TEST_DEBUG: '0',
  ZIG_GLOBAL_CACHE_DIR: '/tmp/datafusion-distributed-zig-global',
  ZIG_LOCAL_CACHE_DIR: '/tmp/datafusion-distributed-zig-local',
});
async function step(label, args) {
  const file = path.join(root, 'raw', `${label}-${Date.now()}.log`);
  fs.writeFileSync(file, `$ cargo ${args.join(' ')}\n`, { flag: 'wx' });
  await new Promise((resolve, reject) => {
    const child = spawn('cargo', args, { cwd, env, stdio: ['ignore', 'pipe', 'pipe'] });
    for (const stream of [child.stdout, child.stderr]) stream.on('data', data => {
      fs.appendFileSync(file, data); process.stdout.write(data);
    });
    child.once('error', reject);
    child.once('close', code => code === 0 ? resolve() : reject(new Error(`${label}: exit ${code}; ${file}`)));
  });
}
const action = process.argv[2];
assert(['validate', 'release'].includes(action));
if (action === 'validate') {
  await step('fmt', ['fmt', '--all', '--check']);
  await step('metrics-tests', ['test', '--locked', '--offline', '--features', 'integration', '--test', 'metrics_collection']);
  await step('remote-filter-test', ['test', '--locked', '--offline', '--features', 'integration', '--test', 'dynamic_filtering',
    'partitioned_join::tests::remote_dynamic_filters', '--', '--exact']);
  await step('clippy', ['clippy', '--locked', '--offline', '--features', 'integration', '--lib', '--test', 'metrics_collection', '--', '-D', 'warnings']);
} else {
  await step('release', ['zigbuild', '--locked', '--offline', '--manifest-path', 'benchmarks/remote-worker/Cargo.toml',
    '--package', 'datafusion-distributed-remote-worker', '--release', '--bin', 'worker', '--target', 'x86_64-unknown-linux-gnu']);
}
