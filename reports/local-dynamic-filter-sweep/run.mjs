import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(root, '../..');
const storage = '/instance_storage/bits_cache/local-dynamic-filter-sweep';
const dataRoot = '/instance_storage/bits_cache/datafusion-benchmark-data';
const manifestFile = path.join(root, 'manifest.json');
const command = process.argv[2] ?? 'screen';
const suiteFilter = process.argv[3];
const hash = file => createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const suites = Object.fromEntries(['tpch', 'tpcds', 'clickbench'].map(suite => [suite, {
  data: `${dataRoot}/${suite}/${suite === 'clickbench' ? '0-100-date32' : 'sf10'}`,
  queries: fs.readdirSync(path.join(repo, 'testdata', suite, 'queries'))
    .filter(f => f.endsWith('.sql')).map(f => f.slice(0, -4))
    .filter(q => suite !== 'tpch' || q !== 'q18')
    .sort((a, b) => +a.slice(1) - +b.slice(1)),
}]));
let manifest;
if (fs.existsSync(manifestFile)) {
  manifest = JSON.parse(fs.readFileSync(manifestFile));
} else {
  manifest = {
    created: new Date().toISOString(), suites, blocks: [],
    machine: { arch: os.arch(), cpu: os.cpus()[0].model,
      cores: os.cpus().length, memory_bytes: os.totalmem() },
    workers: 4, target_partitions: 4, warmups: 1, screening_iterations: 3,
    binaries: Object.fromEntries(['A', 'B'].map(build => [build, {
      sha256: hash(path.join(root, `runner-${build}`)),
      worktree: `${storage}/worktrees/${build}`,
      commit: execFileSync('git', ['rev-parse', 'HEAD'], {
        cwd: `${storage}/worktrees/${build}`, encoding: 'utf8',
      }).trim(),
    }])),
  };
  fs.writeFileSync(path.join(root, 'B.patch'), execFileSync('git', ['diff'], {
    cwd: `${storage}/worktrees/B`,
  }));
  const source = path.join(root, 'source'); fs.mkdirSync(source);
  for (const file of ['dynamic_filter_registry.rs', 'mod.rs',
    'query_coordinator.rs', 'partitioned_dynamic_filter.rs']) {
    fs.copyFileSync(`${storage}/worktrees/B/src/coordinator/${file}`, `${source}/${file}`);
  }
  fs.copyFileSync(`${storage}/worktrees/B/Cargo.lock`, `${source}/Cargo.lock`);
}
const save = () => fs.writeFileSync(manifestFile, JSON.stringify(manifest, null, 2) + '\n');
for (const build of ['A', 'B']) {
  assert.equal(hash(path.join(root, `runner-${build}`)), manifest.binaries[build].sha256,
    `Binary ${build} changed`);
}
let active;
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => {
  manifest.status = 'interrupted'; save();
  if (active) { try { process.kill(-active.pid, signal); } catch {} }
  process.exit(signal === 'SIGINT' ? 130 : 143);
});

async function runBlock({ id, suite, query, build, cases, iterations, reverse }) {
  if (manifest.blocks.some(b => b.id === id && b.finished)) return;
  assert(!fs.existsSync(path.join(root, 'runs', id)), `Unfinished directory exists: ${id}`);
  const output = path.join(root, 'runs', id); fs.mkdirSync(output, { recursive: true });
  const args = ['--data', suites[suite].data, '--output', output, '--suite', suite,
    '--queries', query, '--cases', cases.join(','), '--iterations', String(iterations)];
  if (reverse) args.push('--reverse-order');
  const entry = { id, suite, query, build, cases, iterations, reverse, args,
    started: new Date().toISOString(), load_before: os.loadavg() };
  manifest.blocks.push(entry); manifest.status = `running ${id}`; save();
  console.log(`START ${id} ${cases.join(',')} iterations=${iterations}`);
  const log = fs.openSync(path.join(output, 'run.log'), 'a');
  try {
    const result = await new Promise((resolve, reject) => {
      active = spawn(path.join(root, `runner-${build}`), args, {
        cwd: repo, detached: true, stdio: ['ignore', log, log],
      });
      const watchdog = setTimeout(() => {
        console.error(`WATCHDOG ${id}`);
        try { process.kill(-active.pid, 'SIGKILL'); } catch {}
      }, 40 * 60_000);
      active.once('error', error => { clearTimeout(watchdog); reject(error); });
      active.once('close', (code, signal) => {
        clearTimeout(watchdog); active = undefined; resolve({ code, signal });
      });
    });
    Object.assign(entry, result, { finished: new Date().toISOString(), load_after: os.loadavg() });
    entry.errors = fs.readdirSync(output).filter(f => f.endsWith('-error.json'));
    entry.samples = fs.readdirSync(output).filter(f => /-s\d+\.json$/.test(f)).length;
    save();
    console.log(`END ${id} samples=${entry.samples} errors=${entry.errors.length} exit=${entry.code}`);
  } finally { fs.closeSync(log); }
}

const preparation = JSON.parse(fs.readFileSync(path.join(root, 'data-preparation.json')));
assert(preparation.jobs.every(job => ['complete', 'reused'].includes(job.status)),
  'Finish data preparation before timing');
if (command === 'screen') {
  for (const [suite, { queries }] of Object.entries(suites)) {
    if (suiteFilter && suiteFilter !== suite) continue;
    for (const [i, query] of queries.entries()) {
      for (const build of i % 2 ? ['B', 'A'] : ['A', 'B']) {
        await runBlock({ id: `screen-${suite}-${query}-${build}`, suite, query, build,
          cases: build === 'A' ? ['Control-off', 'A-off', 'Control-on', 'A-on'] : ['B-off', 'B-on'],
          iterations: 3, reverse: i % 2 === 1 });
      }
    }
  }
} else if (command === 'confirm') {
  const candidates = JSON.parse(fs.readFileSync(path.join(root, 'confirmation-plan.json')));
  for (const candidate of candidates) {
    const { suite, query, case: caseName } = candidate;
    if (suiteFilter && suiteFilter !== `${suite}/${query}`) continue;
    const [build, mode] = caseName.split('-');
    for (let round = 1; round <= 2; round++) {
      await runBlock({ id: `confirm-${suite}-${query}-${caseName}-${round}`, suite, query, build,
        cases: [`Control-${mode}`, caseName, `Control-${mode === 'on' ? 'off' : 'on'}`],
        iterations: 10, reverse: round === 2 });
    }
  }
} else { throw new Error(`Unknown command: ${command}`); }
manifest.status = `${command}${suiteFilter ? ` ${suiteFilter}` : ''} complete`;
manifest.updated = new Date().toISOString(); save();
console.log(`COMPLETE ${manifest.status}: ${manifest.blocks.length} blocks recorded`);
