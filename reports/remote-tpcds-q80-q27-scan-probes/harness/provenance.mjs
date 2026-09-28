import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const tools = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const root = path.resolve(tools, '../datafusion-distributed/reports/remote-tpcds-q80-q27-scan-probes');
const buildRoot = '/instance_storage/bits_cache/remote-tpcds-q80-q27-scan-probes';
const registry = '/home/bits/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f';
const sha256 = data => createHash('sha256').update(data).digest('hex');
const files = {
  'datafusion-physical-plan-55.0.0': [
    'Cargo.toml', 'src/metrics.rs', 'src/metrics/scan_probe.rs', 'src/repartition/mod.rs',
  ],
  'datafusion-datasource-parquet-55.0.0': [
    'src/metrics.rs', 'src/opener/mod.rs', 'src/push_decoder.rs', 'src/row_filter.rs',
  ],
};
fs.mkdirSync(path.join(root, 'patches'), { recursive: true });
const dependencies = {};
for (const [crate, paths] of Object.entries(files)) {
  let patch = '';
  const hashes = {};
  for (const file of paths) {
    const original = path.join(registry, crate, file);
    const modified = path.join(buildRoot, 'deps', crate, file);
    const exists = fs.existsSync(original);
    const diff = spawnSync('diff', ['-u', '--label', exists ? `a/${file}` : '/dev/null',
      '--label', `b/${file}`, exists ? original : '/dev/null', modified], { encoding: 'utf8' });
    assert.equal(diff.status, 1, `${crate}/${file} has no intended change or diff failed`);
    patch += diff.stdout;
    hashes[file] = { original: exists ? sha256(fs.readFileSync(original)) : null,
      instrumented: sha256(fs.readFileSync(modified)) };
  }
  const patchPath = `patches/${crate}.patch`;
  if (fs.existsSync(path.join(root, patchPath))) {
    assert.equal(fs.readFileSync(path.join(root, patchPath), 'utf8'), patch);
  } else {
    fs.writeFileSync(path.join(root, patchPath), patch, { flag: 'wx' });
  }
  dependencies[crate] = { patch: patchPath, patch_sha256: sha256(patch), files: hashes,
    registry_checksum: sha256(fs.readFileSync(path.resolve(registry,
      '../../cache/index.crates.io-1949cf8c6b5b557f', `${crate}.crate`))) };
}
const archive = path.join(root, 'raw/dependency-sources.tar.gz');
assert(!fs.existsSync(archive));
execFileSync('tar', ['-czf', archive, '-C', path.join(buildRoot, 'deps'), ...Object.keys(files)]);
fs.copyFileSync(path.join(buildRoot, 'source-A/Cargo.lock'), path.join(root, 'raw/Cargo.lock'), fs.constants.COPYFILE_EXCL);
const manifest = { created_at: new Date().toISOString(), dependencies,
  sources_archive: 'raw/dependency-sources.tar.gz', sources_sha256: sha256(fs.readFileSync(archive)),
  lockfile: 'raw/Cargo.lock', lockfile_sha256: sha256(fs.readFileSync(path.join(root, 'raw/Cargo.lock'))),
  dependency_versions_changed: false, added_dependency_edge: 'datafusion-physical-plan -> existing rustix 1.1.4 (time feature)' };
fs.writeFileSync(path.join(root, 'dependency-provenance.json'), JSON.stringify(manifest, null, 2) + '\n', { flag: 'wx' });
console.log(JSON.stringify(manifest, null, 2));
