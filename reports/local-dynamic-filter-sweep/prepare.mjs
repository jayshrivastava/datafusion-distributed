import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(root, '../..');
const dataRoot = '/instance_storage/bits_cache/datafusion-benchmark-data';
const binary = path.join(repo, 'target/release/dfbench');
const jobs = [
  { name: 'tpcds', data: `${dataRoot}/tpcds/sf10`,
    args: ['prepare-tpcds', '--sf', '10', '--partitions', '16'] },
  { name: 'clickbench', data: `${dataRoot}/clickbench/0-100-date32`,
    args: ['prepare-clickbench', '--partition-start', '0', '--partition-end', '100'] },
];
const manifest = { binary, sha256: createHash('sha256')
  .update(fs.readFileSync(binary)).digest('hex'), jobs };
const save = () => fs.writeFileSync(path.join(root, 'data-preparation.json'),
  JSON.stringify(manifest, null, 2) + '\n');
for (const job of jobs) {
  if (fs.existsSync(path.join(job.data, '_SUCCESS'))) {
    job.status = 'reused'; save(); continue;
  }
  job.status = 'running'; job.started = new Date().toISOString(); save();
  console.log(`PREPARE ${job.name}: ${job.data}`);
  const log = fs.openSync(path.join(root, `prepare-${job.name}.log`), 'a');
  try {
    await new Promise((resolve, reject) => {
      const child = spawn(binary, [...job.args, '--output', job.data], {
        cwd: repo, stdio: ['ignore', log, log],
      });
      child.on('error', reject);
      child.on('close', code => code === 0 ? resolve()
        : reject(new Error(`${job.name} exited ${code}`)));
    });
    job.status = 'complete'; job.finished = new Date().toISOString(); save();
    console.log(`COMPLETE ${job.name}`);
  } catch (error) {
    job.status = 'failed'; job.error = String(error); save(); throw error;
  } finally {
    fs.closeSync(log);
  }
}
