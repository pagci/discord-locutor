import { spawn } from 'node:child_process';
import { readFile, rm, mkdtemp } from 'node:fs/promises';
import { existsSync, realpathSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import libCoverage from 'istanbul-lib-coverage';
import libReport from 'istanbul-lib-report';
import reports from 'istanbul-reports';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const vitest = path.join(root, 'node_modules', 'vitest', 'vitest.mjs');
const modules = realpathSync(path.join(root, 'node_modules'));
const workspaceRoot = realpathSync(root);
const optional = [
  path.join(modules, '@fails-components', 'webtransport'),
  path.join(modules, '@fails-components', 'webtransport-transport-http3-quiche'),
];
const threshold = 86;

function runVitest(args, env = process.env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [vitest, ...args], {
      cwd: root,
      env,
      stdio: 'inherit',
    });
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (signal) reject(new Error(`Vitest interrompido por ${signal}`));
      else if (code) reject(new Error(`Vitest saiu com ${code}`));
      else resolve();
    });
  });
}

function liveEnvironment() {
  const env = { ...process.env, WEBTRANSPORT_LIVE: '1' };
  delete env.WT_LIVE_NODE_MODULES;
  return env;
}

function legacyLiveEnvironment() {
  return {
    ...process.env,
    WEBTRANSPORT_LIVE: '1',
    WT_LIVE_NODE_MODULES: modules,
  };
}

const temp = await mkdtemp(path.join(tmpdir(), 'discord-locutor-coverage-'));
const commonDir = path.join(temp, 'common');
const legacyLiveDir = path.join(temp, 'live-legacy');
const correctiveLiveDir = path.join(temp, 'live-corrective');
const outputDir = path.join(root, 'coverage');
const thresholdsOff = [
  '--coverage.thresholds.lines=0',
  '--coverage.thresholds.statements=0',
  '--coverage.thresholds.functions=0',
  '--coverage.thresholds.branches=0',
];

try {
  await runVitest([
    'run',
    '--coverage',
    '--coverage.reporter=json',
    `--coverage.reportsDirectory=${commonDir}`,
    '--coverage.exclude=server/public/**',
    '--coverage.exclude=**/*.test.js',
    ...thresholdsOff,
  ]);

  if (!optional.every(existsSync)) {
    throw new Error('Coverage live exige os addons opcionais instalados neste workspace.');
  }
  const outside = optional.find(
    (entry) => !realpathSync(entry).startsWith(`${workspaceRoot}${path.sep}`),
  );
  if (outside) throw new Error('Coverage live recusou addon resolvido fora do workspace.');

  await runVitest(
    [
      'run',
      'server/webtransport-binding-capability.webtransport-independent.test.js',
      'server/webtransport-relay-mixed-independent.test.js',
      'server/webtransport-auth-shard-independent.test.js',
      // This case has its own non-instrumented live gate. Its locked 30 s
      // wall-clock deadline is not a coverage threshold and is distorted by
      // V8 collection over 1,100 real QUIC streams.
      '--testNamePattern=^(?!.*atravessa createTransport)',
      '--coverage',
      '--coverage.reporter=json',
      `--coverage.reportsDirectory=${legacyLiveDir}`,
      '--coverage.include=server/webtransport.js',
      '--coverage.include=shared/transport.js',
      '--coverage.include=shared/transport-wire.js',
      ...thresholdsOff,
    ],
    legacyLiveEnvironment(),
  );

  await runVitest(
    [
      'run',
      // The locked live oracle is executed by test:webtransport. Coverage
      // uses the fork unit here because V8 collection distorts its fixed
      // listener-baseline deadline without adding fork-JS coverage.
      'server/webtransport-fork.test.js',
      '--coverage',
      '--coverage.reporter=json',
      `--coverage.reportsDirectory=${correctiveLiveDir}`,
      '--coverage.include=server/webtransport.js',
      '--coverage.include=shared/transport.js',
      '--coverage.include=shared/transport-wire.js',
      '--coverage.include=vendor/fails-components-webtransport/lib/index.node.js',
      '--coverage.include=vendor/fails-components-webtransport/lib/session.js',
      ...thresholdsOff,
    ],
    liveEnvironment(),
  );

  const coverageMap = libCoverage.createCoverageMap({});
  const forkCoverageMap = libCoverage.createCoverageMap({});
  for (const directory of [commonDir, legacyLiveDir, correctiveLiveDir]) {
    const raw = await readFile(path.join(directory, 'coverage-final.json'), 'utf8');
    const app = {};
    const fork = {};
    for (const [file, coverage] of Object.entries(JSON.parse(raw))) {
      if (/[\\/]vendor[\\/]fails-components-webtransport[\\/]lib[\\/]/.test(file)) {
        fork[file] = coverage;
      } else {
        app[file] = coverage;
      }
    }
    coverageMap.merge(app);
    forkCoverageMap.merge(fork);
  }

  if (!forkCoverageMap.files().some((file) => file.endsWith('session.js'))) {
    throw new Error('Coverage V8 do JS alterado no fork não foi coletada.');
  }

  await rm(outputDir, { recursive: true, force: true });
  const context = libReport.createContext({ dir: outputDir, coverageMap });
  reports.create('text').execute(context);
  reports.create('lcovonly').execute(context);
  const forkContext = libReport.createContext({
    dir: path.join(outputDir, 'fork'),
    coverageMap: forkCoverageMap,
  });
  reports.create('text').execute(forkContext);
  reports.create('lcovonly').execute(forkContext);

  const summary = coverageMap.getCoverageSummary().toJSON();
  const failed = ['lines', 'statements', 'functions', 'branches'].filter(
    (metric) => summary[metric].pct < threshold,
  );
  if (failed.length) {
    throw new Error(
      `Coverage abaixo de ${threshold}%: ${failed.map((metric) => `${metric}=${summary[metric].pct}%`).join(', ')}`,
    );
  }
  const forkSummary = forkCoverageMap.getCoverageSummary().toJSON();
  if (
    !forkSummary.lines.covered ||
    !forkSummary.functions.covered ||
    !forkSummary.branches.covered
  ) {
    throw new Error('Coverage V8 do fork não exerceu linhas, funções e branches alterados.');
  }
} finally {
  await rm(temp, { recursive: true, force: true });
}
