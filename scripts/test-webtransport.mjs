import { spawn } from 'node:child_process';
import { existsSync, realpathSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const modules = path.join(root, 'node_modules');
const vitest = path.join(modules, 'vitest', 'vitest.mjs');
const required = [
  path.join(modules, '@fails-components', 'webtransport'),
  path.join(modules, '@fails-components', 'webtransport-transport-http3-quiche'),
  path.join(modules, 'selfsigned'),
];

function processIsAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error?.code === 'ESRCH') return false;
    throw error;
  }
}

async function runTests(files, env) {
  const stdout = [];
  const stderr = [];
  const child = spawn(
    process.execPath,
    [vitest, 'run', ...files, '--reporter=dot', '--maxWorkers=1', '--pool=forks'],
    {
      cwd: root,
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  const pid = child.pid;
  child.stdout.on('data', (chunk) => stdout.push(Buffer.from(chunk)));
  child.stderr.on('data', (chunk) => stderr.push(Buffer.from(chunk)));
  const code = await new Promise((resolve, reject) => {
    child.once('error', reject);
    // `close` only fires after the Vitest process exits and both output pipes
    // close. Workers or native descendants that still own an inherited pipe
    // therefore keep this stage fail-closed instead of leaking into the next.
    child.once('close', (value, signal) => {
      if (signal) reject(new Error(`Gate live interrompido por ${signal}`));
      else resolve(value ?? 1);
    });
  });
  if (stdout.length)
    await new Promise((resolve) => process.stdout.write(Buffer.concat(stdout), resolve));
  if (stderr.length)
    await new Promise((resolve) => process.stdout.write(Buffer.concat(stderr), resolve));
  if (code) throw new Error(`Gate live saiu com ${code}`);
  if (!child.stdout.closed || !child.stderr.closed || (pid && processIsAlive(pid))) {
    throw new Error('Gate live terminou sem quiescencia de processo e pipes');
  }
}

async function runFile(file, env, retryOnce = false) {
  try {
    await runTests([file], env);
  } catch (firstError) {
    if (!retryOnce) throw firstError;
    console.log(`[flake] ${file} falhou; repetindo uma vez em processo novo`);
    try {
      await runTests([file], env);
    } catch (secondError) {
      throw new AggregateError(
        [firstError, secondError],
        `${file} falhou em duas tentativas quiescentes`,
        { cause: secondError },
      );
    }
  }
}

if (!required.every(existsSync)) {
  console.error('Gate live exige os addons opcionais e selfsigned instalados neste workspace.');
  process.exitCode = 1;
} else {
  const workspaceModules = realpathSync(modules);
  const workspaceRoot = realpathSync(root);
  const outside = required.find(
    (entry) => !realpathSync(entry).startsWith(`${workspaceRoot}${path.sep}`),
  );
  if (outside) {
    console.error('Gate live recusou dependência resolvida fora do workspace.');
    process.exitCode = 1;
  } else {
    if (!existsSync(vitest)) throw new Error('Vitest local ausente no workspace');
    const legacyEnv = {
      ...process.env,
      WEBTRANSPORT_LIVE: '1',
      WT_LIVE_NODE_MODULES: workspaceModules,
    };
    const correctiveEnv = { ...process.env, WEBTRANSPORT_LIVE: '1' };
    delete correctiveEnv.WT_LIVE_NODE_MODULES;
    try {
      for (const file of [
        'server/webtransport-binding-capability.webtransport-independent.test.js',
        'server/webtransport-relay-mixed-independent.test.js',
        'server/webtransport-auth-shard-independent.test.js',
      ]) {
        await runFile(
          file,
          legacyEnv,
          file === 'server/webtransport-binding-capability.webtransport-independent.test.js',
        );
      }
      for (const file of [
        'server/webtransport-stuck-close.webtransport-independent.test.js',
        'server/webtransport-fork.test.js',
      ]) {
        await runFile(file, correctiveEnv);
      }
    } catch (error) {
      console.error(error?.message ?? error);
      process.exitCode = 1;
    }
  }
}
