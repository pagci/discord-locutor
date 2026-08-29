import { spawn, spawnSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import {
  cp,
  mkdir,
  mkdtemp,
  open,
  readFile,
  readdir,
  rename,
  rm,
  stat,
  writeFile,
} from 'node:fs/promises';
import { cpus, freemem, tmpdir, totalmem } from 'node:os';
import path from 'node:path';
import { monitorEventLoopDelay, performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const selfPath = fileURLToPath(import.meta.url);
const probePath = path.join(root, 'scripts', 'webtransport-drain-probe.mjs');
const vitestPath = path.join(root, 'node_modules', 'vitest', 'vitest.mjs');
const lockedPath = 'server/webtransport-stuck-close.webtransport-independent.test.js';
const SAMPLE_MS = 250;
const SERVER_SAMPLE_MS = 1000;
const WALL_MS = 60 * 60_000;
const MEASURE_STOP_MS = 55 * 60_000;
const CHILD_LIMIT_MS = 90_000;
const QUIET_WINDOW_MS = 15_000;
const STABILIZE_LIMIT_MS = 120_000;
const MAX_ATTEMPTS = 16;
const REQUIRED_VALID_PAIRS = 12;
const ARM_SEED = 'sprint-02d-20260827-2';
const INSTRUMENT_SEED = 'sprint-02d-instrument-20260827-2';
const expectedArmOrders = [
  'AB',
  'BA',
  'BA',
  'AB',
  'BA',
  'BA',
  'BA',
  'BA',
  'AB',
  'AB',
  'AB',
  'AB',
  'BA',
  'AB',
  'AB',
  'AB',
];
const expectedInstrumentOrders = {
  A: [
    'LP',
    'PL',
    'LP',
    'PL',
    'LP',
    'PL',
    'PL',
    'PL',
    'LP',
    'LP',
    'LP',
    'PL',
    'LP',
    'PL',
    'PL',
    'LP',
  ],
  B: [
    'LP',
    'LP',
    'PL',
    'LP',
    'PL',
    'PL',
    'LP',
    'PL',
    'PL',
    'PL',
    'LP',
    'LP',
    'PL',
    'LP',
    'LP',
    'LP',
  ],
};
const expectedLocks = new Map([
  [
    'server/webtransport-binding-capability.webtransport-independent.test.js',
    'a57b46066f0da10d98cf8f4fb2c795cdc96adb3170aa3b7c28b555a0e2f09386',
  ],
  [
    'shared/transport-fallback-independent.test.js',
    'd0f5df848a0d2e769cf0b945abcb6af50a04ae52498e5c2b289fee63781d5033',
  ],
  [
    'shared/transport-barrier-hol-independent.test.js',
    '24ba3944d4f303b25d3c31a5f14a8613a9965e31db71fa2d2e84f87f8efb2739',
  ],
  [
    'server/webtransport-relay-mixed-independent.test.js',
    'd646d87cba7dd7194ef1aa137114ef46b234d43fdad0bf68eaad70a67ed02a39',
  ],
  [
    'server/webtransport-auth-shard-independent.test.js',
    'ee8def5ae95a1d9a955edfd6dbd0c039a6398e006e014ec320217dce457bfe24',
  ],
  [
    'scripts/verify-ws-only-independent.mjs',
    '71bc7213f7864b08a69cd31395edbfbe5e7cbe9e564124d8dd2c8eb5e57bb578',
  ],
  [
    'shared/transport-lifecycle-roles-independent.test.js',
    '7802f619346c2ebff7de5c77d0186b7b5a7e16b57d9a2ae17cc1863ff7aaf92d',
  ],
  [lockedPath, '34a3226a1f20c4090fe524f312f40398c80b1cd879f757610f7df71c874d7dd0'],
]);

function parseArgs() {
  const values = { mode: 'supervisor', output: null, startedAt: null, runId: null };
  for (let index = 2; index < process.argv.length; index++) {
    const argument = process.argv[index];
    if (argument === '--worker') values.mode = 'worker';
    else if (argument === '--burner') values.mode = 'burner';
    else if (argument === '--self-check') values.mode = 'self-check';
    else if (argument === '--sink-canary-worker') values.mode = 'sink-canary-worker';
    else if (argument === '--output') values.output = path.resolve(process.argv[++index]);
    else if (argument === '--started-at') values.startedAt = Number(process.argv[++index]);
    else if (argument === '--run-id') values.runId = process.argv[++index];
  }
  return values;
}

function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

function percentile(values, fraction) {
  if (!values.length) return null;
  const sorted = [...values].sort((left, right) => left - right);
  const rank = Math.max(1, Math.ceil(fraction * sorted.length));
  return sorted[rank - 1];
}

function summarizeNumbers(values) {
  if (!values.length) return { count: 0 };
  return {
    count: values.length,
    min: Math.min(...values),
    p50: percentile(values, 0.5),
    p90: percentile(values, 0.9),
    p95: percentile(values, 0.95),
    p99: percentile(values, 0.99),
    max: Math.max(...values),
  };
}

const SINK_SENTINEL = '.run-sentinel.json';
const SINK_ERROR = 'instrument-error: sink-lost-or-replaced';

function fileIdentity(info) {
  return `${info.dev}:${info.ino}:${info.birthtimeMs}`;
}

async function pathExists(file) {
  try {
    await stat(file);
    return true;
  } catch (error) {
    if (error?.code === 'ENOENT') return false;
    throw error;
  }
}

function sinkError(cause) {
  const error = new Error(`${SINK_ERROR}${cause ? ` (${cause})` : ''}`);
  error.code = 'SINK_LOST_OR_REPLACED';
  return error;
}

async function atomicStandaloneJson(file, value) {
  const temporary = `${file}.${process.pid}.${randomUUID()}.tmp`;
  const handle = await open(temporary, 'wx');
  try {
    await handle.writeFile(`${JSON.stringify(value, null, 2)}\n`);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(temporary, file);
}

function safeSinkPath(rootPath, relativePath) {
  if (!relativePath || path.isAbsolute(relativePath)) throw sinkError('invalid-relative-path');
  const resolved = path.resolve(rootPath, relativePath);
  const prefix = `${rootPath}${path.sep}`;
  if (!resolved.startsWith(prefix)) throw sinkError('path-escape');
  return resolved;
}

async function parseJsonl(file) {
  if (!(await pathExists(file))) return { count: 0, size: 0, terminalCount: 0, records: [] };
  const raw = await readFile(file, 'utf8');
  const lines = raw.split(/\r?\n/).filter(Boolean);
  const records = lines.map((line, index) => {
    const record = JSON.parse(line);
    if (record.recordSequence !== index + 1) throw sinkError('jsonl-sequence');
    return record;
  });
  return {
    count: records.length,
    size: Buffer.byteLength(raw),
    terminalCount: records.filter((record) => record.type === 'terminal').length,
    records,
  };
}

class SinkGuard {
  static async create(rootPath, runId = randomUUID()) {
    const sentinelPath = path.join(rootPath, SINK_SENTINEL);
    const sentinel = { schemaVersion: 1, runId, createdAt: new Date().toISOString() };
    const handle = await open(sentinelPath, 'wx');
    try {
      await handle.writeFile(`${JSON.stringify(sentinel)}\n`);
      await handle.sync();
    } finally {
      await handle.close();
    }
    await mkdir(path.join(rootPath, 'raw'), { recursive: false });
    return SinkGuard.attach(rootPath, runId);
  }

  static async attach(rootPath, runId) {
    try {
      const resolvedRoot = path.resolve(rootPath);
      const sentinelPath = path.join(resolvedRoot, SINK_SENTINEL);
      const [rootInfo, rawInfo, sentinelInfo, sentinelRaw] = await Promise.all([
        stat(resolvedRoot),
        stat(path.join(resolvedRoot, 'raw')),
        stat(sentinelPath),
        readFile(sentinelPath, 'utf8'),
      ]);
      const sentinel = JSON.parse(sentinelRaw);
      if (!runId || sentinel.runId !== runId) throw sinkError('run-id');
      const jsonl = await parseJsonl(path.join(resolvedRoot, 'raw.jsonl'));
      const guard = new SinkGuard();
      guard.root = resolvedRoot;
      guard.runId = runId;
      guard.sentinelRaw = sentinelRaw;
      guard.rootIdentity = fileIdentity(rootInfo);
      guard.rawIdentity = fileIdentity(rawInfo);
      guard.sentinelIdentity = fileIdentity(sentinelInfo);
      guard.jsonlPath = path.join(resolvedRoot, 'raw.jsonl');
      guard.jsonlSize = jsonl.size;
      guard.recordSequence = jsonl.count;
      guard.jsonlIdentity = jsonl.count ? fileIdentity(await stat(guard.jsonlPath)) : null;
      return guard;
    } catch (error) {
      if (error?.code === 'SINK_LOST_OR_REPLACED') throw error;
      throw sinkError(error?.code ?? error?.message);
    }
  }

  async assertIdentity() {
    try {
      const sentinelPath = path.join(this.root, SINK_SENTINEL);
      const [rootInfo, rawInfo, sentinelInfo, sentinelRaw] = await Promise.all([
        stat(this.root),
        stat(path.join(this.root, 'raw')),
        stat(sentinelPath),
        readFile(sentinelPath, 'utf8'),
      ]);
      if (
        fileIdentity(rootInfo) !== this.rootIdentity ||
        fileIdentity(rawInfo) !== this.rawIdentity ||
        fileIdentity(sentinelInfo) !== this.sentinelIdentity ||
        sentinelRaw !== this.sentinelRaw ||
        JSON.parse(sentinelRaw).runId !== this.runId
      ) {
        throw sinkError('identity');
      }
    } catch (error) {
      if (error?.code === 'SINK_LOST_OR_REPLACED') throw error;
      throw sinkError(error?.code ?? error?.message);
    }
  }

  async assertJsonl() {
    await this.assertIdentity();
    if (this.recordSequence === 0) {
      if (await pathExists(this.jsonlPath)) throw sinkError('unexpected-jsonl');
      return;
    }
    try {
      const info = await stat(this.jsonlPath);
      if (fileIdentity(info) !== this.jsonlIdentity || info.size !== this.jsonlSize) {
        throw sinkError('jsonl-size-or-identity');
      }
      const parsed = await parseJsonl(this.jsonlPath);
      if (parsed.count !== this.recordSequence || parsed.size !== this.jsonlSize) {
        throw sinkError('jsonl-monotonicity');
      }
    } catch (error) {
      if (error?.code === 'SINK_LOST_OR_REPLACED') throw error;
      throw sinkError(error?.code ?? error?.message);
    }
  }

  async append(value) {
    await this.assertJsonl();
    const recordSequence = this.recordSequence + 1;
    const encoded = Buffer.from(`${JSON.stringify({ ...value, recordSequence })}\n`);
    let handle;
    try {
      handle = await open(this.jsonlPath, this.recordSequence === 0 ? 'wx' : 'r+');
      const before = await handle.stat();
      if (this.recordSequence > 0) {
        if (fileIdentity(before) !== this.jsonlIdentity || before.size !== this.jsonlSize) {
          throw sinkError('jsonl-open-race');
        }
      } else if (before.size !== 0) {
        throw sinkError('jsonl-created-nonempty');
      }
      await handle.write(encoded, 0, encoded.length, this.jsonlSize);
      await handle.sync();
      const after = await handle.stat();
      const expectedSize = this.jsonlSize + encoded.length;
      if (after.size !== expectedSize) throw sinkError('jsonl-write-size');
      this.jsonlIdentity = fileIdentity(after);
      this.jsonlSize = expectedSize;
      this.recordSequence = recordSequence;
    } catch (error) {
      if (error?.code === 'SINK_LOST_OR_REPLACED') throw error;
      throw sinkError(error?.code ?? error?.message);
    } finally {
      await handle?.close();
    }
    await this.assertJsonl();
    return recordSequence;
  }

  async write(relativePath, value) {
    await this.assertIdentity();
    const file = safeSinkPath(this.root, relativePath);
    const temporary = `${file}.${process.pid}.${randomUUID()}.tmp`;
    let handle;
    try {
      handle = await open(temporary, 'wx');
      await handle.writeFile(value);
      await handle.sync();
    } catch (error) {
      if (error?.code === 'SINK_LOST_OR_REPLACED') throw error;
      throw sinkError(error?.code ?? error?.message);
    } finally {
      await handle?.close();
    }
    await this.assertIdentity();
    try {
      await rename(temporary, file);
    } catch (error) {
      throw sinkError(error?.code ?? error?.message);
    }
    await this.assertIdentity();
  }

  writeJson(relativePath, value) {
    return this.write(relativePath, `${JSON.stringify(value, null, 2)}\n`);
  }
}

function processIsAlive(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error?.code === 'ESRCH') return false;
    return true;
  }
}

function terminateTree(child) {
  if (!child?.pid || !processIsAlive(child.pid)) return;
  if (process.platform === 'win32') {
    spawnSync('taskkill.exe', ['/PID', String(child.pid), '/T', '/F'], {
      stdio: 'ignore',
      windowsHide: true,
    });
  } else {
    child.kill('SIGKILL');
  }
}

class CpuSampler {
  constructor() {
    this.samples = [];
    this.previous = cpus();
    this.timer = null;
  }

  start() {
    this.timer = setInterval(() => this.sample(), SAMPLE_MS);
    this.timer.unref?.();
  }

  sample() {
    const current = cpus();
    let totalDelta = 0;
    let idleDelta = 0;
    for (let index = 0; index < current.length; index++) {
      const before = this.previous[index].times;
      const after = current[index].times;
      const beforeTotal = Object.values(before).reduce((sum, value) => sum + value, 0);
      const afterTotal = Object.values(after).reduce((sum, value) => sum + value, 0);
      totalDelta += afterTotal - beforeTotal;
      idleDelta += after.idle - before.idle;
    }
    this.previous = current;
    if (totalDelta <= 0) return;
    this.samples.push({
      at: Date.now(),
      busyPercent: (100 * (totalDelta - idleDelta)) / totalDelta,
      freeMemory: freemem(),
      totalMemory: totalmem(),
      rss: process.memoryUsage().rss,
    });
  }

  stop() {
    clearInterval(this.timer);
  }

  since(at) {
    return this.samples.filter((sample) => sample.at >= at);
  }

  between(start, end = Date.now()) {
    return this.samples.filter((sample) => sample.at >= start && sample.at <= end);
  }
}

class BurnerPool {
  constructor() {
    this.children = [];
  }

  setCount(target) {
    const wanted = Math.max(0, Math.min(cpus().length * 2, target));
    while (this.children.length < wanted) {
      const child = spawn(process.execPath, [selfPath, '--burner'], {
        cwd: root,
        stdio: 'ignore',
        windowsHide: true,
      });
      this.children.push(child);
    }
    while (this.children.length > wanted) {
      const child = this.children.pop();
      child.kill();
    }
  }

  snapshot() {
    const children = this.children.map((child) => ({
      pid: child.pid,
      alive: processIsAlive(child.pid),
      exitCode: child.exitCode,
      signalCode: child.signalCode,
    }));
    return {
      count: children.length,
      aliveCount: children.filter((child) => child.alive).length,
      children,
      dutyCycle: children.length ? { workMs: 25, yield: 'setImmediate', fraction: 1 } : null,
    };
  }

  async stop() {
    const children = this.children.splice(0);
    for (const child of children) child.kill();
    await Promise.all(
      children.map(
        (child) =>
          new Promise((resolve) => {
            if (child.exitCode !== null || child.signalCode) resolve();
            else child.once('close', resolve);
            setTimeout(() => {
              terminateTree(child);
              resolve();
            }, 2000).unref?.();
          }),
      ),
    );
  }
}

function runBurner() {
  const buffer = Buffer.alloc(64 * 1024, 0x5a);
  let stopped = false;
  process.once('SIGTERM', () => (stopped = true));
  process.once('SIGINT', () => (stopped = true));
  const cycle = () => {
    if (stopped) return;
    const deadline = performance.now() + 25;
    while (performance.now() < deadline) createHash('sha256').update(buffer).digest();
    setImmediate(cycle);
  };
  cycle();
}

function serverTelemetryScript() {
  return String.raw`
$ErrorActionPreference = 'Stop'
Add-Type @'
using System;
using System.Runtime.InteropServices;
public static class DrainIoNative {
  [StructLayout(LayoutKind.Sequential)]
  public struct IO_COUNTERS {
    public UInt64 ReadOperationCount;
    public UInt64 WriteOperationCount;
    public UInt64 OtherOperationCount;
    public UInt64 ReadTransferCount;
    public UInt64 WriteTransferCount;
    public UInt64 OtherTransferCount;
  }
  [DllImport("kernel32.dll", SetLastError=true)]
  static extern bool GetProcessIoCounters(IntPtr hProcess, out IO_COUNTERS counters);
  public static IO_COUNTERS Read(IntPtr handle) {
    IO_COUNTERS value;
    if (!GetProcessIoCounters(handle, out value)) throw new System.ComponentModel.Win32Exception();
    return value;
  }
}
'@
$listener = Get-NetTCPConnection -State Listen -LocalPort 3100 | Select-Object -First 1
if (-not $listener) { throw 'listener :3100 absent' }
$owner = [int]$listener.OwningProcess
$process = Get-Process -Id $owner
$start = $process.StartTime.ToUniversalTime().ToString('o')
while ($true) {
  $process.Refresh()
  if ($process.HasExited) { throw 'server :3100 exited' }
  $io = [DrainIoNative]::Read($process.Handle)
  $network = [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties()
  $listeners = @($network.GetActiveTcpListeners() | Where-Object { $_.Port -eq 3100 }).Count
  $connections = @($network.GetActiveTcpConnections() | Where-Object {
    $_.LocalEndPoint.Port -eq 3100 -and $_.State -eq [System.Net.NetworkInformation.TcpState]::Established
  }).Count
  $row = [ordered]@{
    at = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    pid = $owner
    start = $start
    cpuMs = $process.TotalProcessorTime.TotalMilliseconds
    ioBytes = [double]($io.ReadTransferCount + $io.WriteTransferCount + $io.OtherTransferCount)
    listeners = $listeners
    established = $connections
  }
  [Console]::Out.WriteLine(($row | ConvertTo-Json -Compress))
  [Console]::Out.Flush()
  Start-Sleep -Milliseconds ${SERVER_SAMPLE_MS}
}
`;
}

class ServerTelemetry {
  constructor() {
    this.child = null;
    this.samples = [];
    this.stderr = '';
    this.ready = null;
  }

  async start() {
    let resolveReady;
    let rejectReady;
    this.ready = new Promise((resolve, reject) => {
      resolveReady = resolve;
      rejectReady = reject;
    });
    this.child = spawn(
      'powershell.exe',
      ['-NoLogo', '-NoProfile', '-NonInteractive', '-Command', serverTelemetryScript()],
      { cwd: root, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true },
    );
    let pending = '';
    this.child.stdout.setEncoding('utf8');
    this.child.stdout.on('data', (chunk) => {
      pending += chunk;
      const lines = pending.split(/\r?\n/);
      pending = lines.pop() ?? '';
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          this.samples.push(JSON.parse(line));
          resolveReady(this.samples[0]);
        } catch (error) {
          rejectReady(error);
        }
      }
    });
    this.child.stderr.on('data', (chunk) => (this.stderr += chunk));
    this.child.once('error', rejectReady);
    this.child.once('close', (code) => {
      if (!this.samples.length)
        rejectReady(new Error(`server telemetry exited ${code}: ${this.stderr}`));
    });
    return Promise.race([
      this.ready,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('server telemetry ready timeout')), 10_000),
      ),
    ]);
  }

  between(start, end = Date.now()) {
    return this.samples.filter((sample) => sample.at >= start && sample.at <= end);
  }

  async stop() {
    if (!this.child) return;
    const child = this.child;
    child.kill();
    await new Promise((resolve) => {
      if (child.exitCode !== null || child.signalCode) resolve();
      else child.once('close', resolve);
      setTimeout(() => {
        terminateTree(child);
        resolve();
      }, 3000).unref?.();
    });
  }
}

function longestRunAtOrBelow(samples, threshold) {
  let current = null;
  let best = null;
  for (const sample of samples) {
    const contiguous = current && sample.at - current.end <= SAMPLE_MS * 2;
    if (sample.busyPercent <= threshold) {
      current = contiguous
        ? { ...current, end: sample.at, sampleCount: current.sampleCount + 1 }
        : { start: sample.at, end: sample.at, sampleCount: 1 };
      const coveredMsEstimate = current.end - current.start + SAMPLE_MS;
      if (!best || coveredMsEstimate > best.coveredMsEstimate) {
        best = { ...current, coveredMsEstimate };
      }
    } else {
      current = null;
    }
  }
  return {
    threshold,
    samplePeriodMs: SAMPLE_MS,
    maxGapMs: SAMPLE_MS * 2,
    ...(best ?? { start: null, end: null, sampleCount: 0, coveredMsEstimate: 0 }),
  };
}

function cpuMetrics(samples) {
  const busy = samples.map((sample) => sample.busyPercent);
  const summary = summarizeNumbers(busy);
  return {
    ...summary,
    in60to90: busy.length
      ? busy.filter((value) => value >= 60 && value <= 90).length / busy.length
      : 0,
    freeMemoryMin: samples.length ? Math.min(...samples.map((sample) => sample.freeMemory)) : null,
    rssMax: samples.length ? Math.max(...samples.map((sample) => sample.rss)) : null,
    atOrBelowRuns: Object.fromEntries(
      [20, 30, 40, 50].map((threshold) => [threshold, longestRunAtOrBelow(samples, threshold)]),
    ),
  };
}

function serverMetrics(samples, baseline) {
  if (samples.length < 2) return { valid: false, reason: 'server-telemetry-insufficient' };
  const first = samples[0];
  const last = samples.at(-1);
  const identityValid = samples.every(
    (sample) =>
      sample.pid === baseline.pid &&
      sample.start === baseline.start &&
      sample.listeners === baseline.listeners,
  );
  const established = [...new Set(samples.map((sample) => sample.established))];
  const cpuMs = last.cpuMs - first.cpuMs;
  const ioBytes = last.ioBytes - first.ioBytes;
  return {
    valid: identityValid && established.length === 1 && established[0] === baseline.established,
    identityValid,
    established,
    cpuMs,
    ioBytes,
    count: samples.length,
  };
}

function validArmCpu(arm, metrics) {
  if (!metrics.count) return false;
  if (arm === 'A') return metrics.p95 <= 20;
  return metrics.p50 >= 70 && metrics.p50 <= 80 && metrics.p95 <= 95 && metrics.in60to90 >= 0.8;
}

function validServerExecution(metrics) {
  return metrics.valid && metrics.cpuMs <= 500 && metrics.ioBytes <= 1024 * 1024;
}

async function sha256File(file) {
  return sha256(await readFile(file));
}

function excludedManifestPath(relative) {
  const normalized = relative.replaceAll('\\', '/').toLowerCase();
  return (
    normalized.startsWith('.git/') ||
    normalized.includes('/node_modules/') ||
    normalized.startsWith('node_modules/') ||
    normalized.includes('/dist/') ||
    normalized.startsWith('dist/') ||
    normalized.includes('/coverage/') ||
    normalized.startsWith('coverage/') ||
    normalized.startsWith('ensaio-resultados/')
  );
}

async function candidateSnapshot() {
  const listed = spawnSync('git', ['ls-files', '-co', '--exclude-standard', '-z'], {
    cwd: root,
    encoding: 'utf8',
    windowsHide: true,
  });
  if (listed.status !== 0) throw new Error(`git ls-files failed: ${listed.stderr}`);
  const files = listed.stdout
    .split('\0')
    .filter(Boolean)
    .filter((relative) => !excludedManifestPath(relative))
    .sort();
  const rows = [];
  for (const relative of files) {
    const absolute = path.join(root, relative);
    const metadata = await stat(absolute);
    if (!metadata.isFile()) continue;
    rows.push(`${relative.replaceAll('\\', '/')}\0${await sha256File(absolute)}`);
  }
  const locks = {};
  for (const [relative, expected] of expectedLocks) {
    const actual = await sha256File(path.join(root, relative));
    locks[relative] = actual;
    if (actual !== expected) throw new Error(`locked hash drift: ${relative}=${actual}`);
  }
  let envHash = null;
  try {
    envHash = await sha256File(path.join(root, '.env'));
  } catch {
    // A missing .env remains an explicit null fingerprint.
  }
  return {
    manifest: sha256(rows.join('\n')),
    fileCount: rows.length,
    exclusions: [
      '.git',
      'node_modules',
      'dist',
      'coverage',
      'diagnostic-output',
      'ensaio-resultados/',
    ],
    locks,
    envHash,
  };
}

function armOrder(attempt) {
  const digest = createHash('sha256').update(`${ARM_SEED}:${attempt}`).digest();
  return (digest[0] & 1) === 0 ? 'AB' : 'BA';
}

function instrumentOrder(attempt, arm) {
  const digest = createHash('sha256').update(`${INSTRUMENT_SEED}:${attempt}:${arm}`).digest();
  return (digest[0] & 1) === 0 ? 'PL' : 'LP';
}

function verifySeeds() {
  const actualArms = Array.from({ length: MAX_ATTEMPTS }, (_, index) => armOrder(index + 1));
  const actualA = Array.from({ length: MAX_ATTEMPTS }, (_, index) =>
    instrumentOrder(index + 1, 'A'),
  );
  const actualB = Array.from({ length: MAX_ATTEMPTS }, (_, index) =>
    instrumentOrder(index + 1, 'B'),
  );
  if (JSON.stringify(actualArms) !== JSON.stringify(expectedArmOrders)) {
    throw new Error(`arm seed mismatch: ${actualArms.join(',')}`);
  }
  if (JSON.stringify(actualA) !== JSON.stringify(expectedInstrumentOrders.A)) {
    throw new Error(`instrument A seed mismatch: ${actualA.join(',')}`);
  }
  if (JSON.stringify(actualB) !== JSON.stringify(expectedInstrumentOrders.B)) {
    throw new Error(`instrument B seed mismatch: ${actualB.join(',')}`);
  }
  return { actualArms, actualA, actualB };
}

async function writeRaw(sink, id, stream, value) {
  const relative = path.join('raw', `${id}.${stream}.log`);
  await sink.write(relative, value);
  return { file: relative.replaceAll('\\', '/'), sha256: sha256(value) };
}

async function runChild({ sink, id, command, args, env, limitMs }) {
  const stdout = [];
  const stderr = [];
  const child = spawn(command, args, {
    cwd: root,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
    windowsHide: true,
  });
  const pid = child.pid;
  child.stdout.on('data', (chunk) => stdout.push(Buffer.from(chunk)));
  child.stderr.on('data', (chunk) => stderr.push(Buffer.from(chunk)));
  let timedOut = false;
  const timer = setTimeout(
    () => {
      timedOut = true;
      terminateTree(child);
    },
    Math.max(1, limitMs),
  );
  const result = await new Promise((resolve) => {
    child.once('error', (error) => resolve({ code: null, signal: null, error }));
    child.once('close', (code, signal) => resolve({ code, signal, error: null }));
  });
  clearTimeout(timer);
  const stdoutText = Buffer.concat(stdout).toString('utf8');
  const stderrText = Buffer.concat(stderr).toString('utf8');
  const raw = {
    stdout: await writeRaw(sink, id, 'stdout', stdoutText),
    stderr: await writeRaw(sink, id, 'stderr', stderrText),
  };
  return {
    ...result,
    pid,
    timedOut,
    pipesClosed: child.stdout.closed && child.stderr.closed,
    processAliveAfterClose: processIsAlive(pid),
    stdoutText,
    stderrText,
    raw,
  };
}

function parseProbe(child) {
  const line = child.stdoutText
    .split(/\r?\n/)
    .filter((candidate) => candidate.startsWith('DRAIN_JSON '))
    .at(-1);
  if (!line) return { ok: false, error: 'DRAIN_JSON missing' };
  try {
    return JSON.parse(line.slice('DRAIN_JSON '.length));
  } catch (error) {
    return { ok: false, error: `DRAIN_JSON invalid: ${error.message}` };
  }
}

function eventLoopMetrics(histogram) {
  return {
    minMs: Number.isFinite(histogram.min) ? histogram.min / 1e6 : null,
    maxMs: Number.isFinite(histogram.max) ? histogram.max / 1e6 : null,
    p50Ms: histogram.percentile(50) / 1e6,
    p95Ms: histogram.percentile(95) / 1e6,
    p99Ms: histogram.percentile(99) / 1e6,
  };
}

async function measureWindow(context, label, action) {
  const start = Date.now();
  const burnersStart = context.burners.snapshot();
  context.loopDelay.reset();
  const value = await action();
  const end = Date.now();
  const burnersEnd = context.burners.snapshot();
  const cpu = cpuMetrics(context.cpu.between(start, end));
  const server = serverMetrics(context.server.between(start, end), context.serverBaseline);
  return {
    label,
    start,
    end,
    durationMs: end - start,
    cpu,
    server,
    eventLoop: eventLoopMetrics(context.loopDelay),
    burners: { start: burnersStart, end: burnersEnd },
    value,
  };
}

async function passiveWindow(context, label, milliseconds) {
  return measureWindow(
    context,
    label,
    () => new Promise((resolve) => setTimeout(resolve, milliseconds)),
  );
}

function evaluateStabilizationCpu(arm, cpu, options = {}) {
  if (arm === 'A') {
    const limits = { p95Max: 20 };
    const checks = {
      p95: { observed: cpu.p95, operator: '<=', limit: limits.p95Max, pass: cpu.p95 <= 20 },
    };
    return { accepted: checks.p95.pass, limits, checks };
  }
  const limits = {
    p50Min: options.minimum ?? 70,
    p50Max: options.maximum ?? 80,
    p95Max: options.p95Maximum ?? 95,
    in60to90Min: 0.8,
  };
  const checks = {
    p50Min: {
      observed: cpu.p50,
      operator: '>=',
      limit: limits.p50Min,
      pass: cpu.p50 >= limits.p50Min,
    },
    p50Max: {
      observed: cpu.p50,
      operator: '<=',
      limit: limits.p50Max,
      pass: cpu.p50 <= limits.p50Max,
    },
    p95: {
      observed: cpu.p95,
      operator: '<=',
      limit: limits.p95Max,
      pass: cpu.p95 <= limits.p95Max,
    },
    in60to90: {
      observed: cpu.in60to90,
      operator: '>=',
      limit: limits.in60to90Min,
      pass: cpu.in60to90 >= limits.in60to90Min,
    },
  };
  return { accepted: Object.values(checks).every((check) => check.pass), limits, checks };
}

async function persistStabilizationCandidate(
  context,
  descriptor,
  candidateIndex,
  candidate,
  evaluation,
) {
  const pairKind = descriptor.pilot ? 'pilot' : descriptor.calibration ? 'calibration' : 'attempt';
  const recordSequence = await context.sink.append({
    type: 'stabilization-candidate',
    pairKind,
    pairId: descriptor.id,
    attempt: descriptor.attempt ?? null,
    arm: descriptor.arm,
    armOrder: descriptor.armOrder,
    instrumentOrder: descriptor.instrumentOrder,
    candidateIndex,
    accepted: evaluation.accepted,
    limits: evaluation.limits,
    checks: evaluation.checks,
    window: candidate,
  });
  const reference = {
    recordSequence,
    pairKind,
    pairId: descriptor.id,
    arm: descriptor.arm,
    candidateIndex,
    accepted: evaluation.accepted,
    start: candidate.start,
    end: candidate.end,
  };
  context.stabilizationRecords.push(reference);
  return reference;
}

async function stabilizeA(context, descriptor) {
  const deadline = Date.now() + STABILIZE_LIMIT_MS;
  const candidates = [];
  while (Date.now() < deadline) {
    const candidateIndex = candidates.length + 1;
    const candidate = await passiveWindow(context, 'A-stabilization', QUIET_WINDOW_MS);
    const evaluation = evaluateStabilizationCpu('A', candidate.cpu);
    candidates.push(
      await persistStabilizationCandidate(
        context,
        descriptor,
        candidateIndex,
        candidate,
        evaluation,
      ),
    );
    if (evaluation.accepted) return { window: candidate, candidates, deadline, timedOut: false };
  }
  return { window: null, candidates, deadline, timedOut: true };
}

async function stabilizeB(context, descriptor, minimum = 70, maximum = 80, p95Maximum = 95) {
  if (!context.burners.children.length)
    context.burners.setCount(Math.max(1, Math.floor(cpus().length * 0.6)));
  const deadline = Date.now() + STABILIZE_LIMIT_MS;
  const candidates = [];
  while (Date.now() < deadline) {
    const candidateIndex = candidates.length + 1;
    const candidate = await passiveWindow(context, 'B-stabilization', QUIET_WINDOW_MS);
    const evaluation = evaluateStabilizationCpu('B', candidate.cpu, {
      minimum,
      maximum,
      p95Maximum,
    });
    candidates.push(
      await persistStabilizationCandidate(
        context,
        descriptor,
        candidateIndex,
        candidate,
        evaluation,
      ),
    );
    if (evaluation.accepted) return { window: candidate, candidates, deadline, timedOut: false };
    const current = context.burners.children.length || 1;
    const target = (minimum + maximum) / 2;
    let next = Math.round((current * target) / Math.max(candidate.cpu.p50 ?? 1, 1));
    if (next === current) next += (candidate.cpu.p50 ?? 0) < minimum ? 1 : -1;
    context.burners.setCount(next);
  }
  return { window: null, candidates, deadline, timedOut: true };
}

function stabilizationTimeoutEvidence(stabilizationResult) {
  return {
    count: stabilizationResult.candidates.length,
    recordSequences: stabilizationResult.candidates.map((candidate) => candidate.recordSequence),
    candidates: stabilizationResult.candidates,
    deadline: stabilizationResult.deadline,
  };
}

async function executeInstrument(context, descriptor, instrument) {
  const id = `${descriptor.id}-${descriptor.arm}-${instrument}`;
  const remaining = context.measureStopAt - Date.now();
  if (remaining <= 0) throw new Error('measurement-wall-stop');
  const limitMs = Math.min(CHILD_LIMIT_MS, remaining);
  return measureWindow(context, `${descriptor.arm}-${instrument}`, async () => {
    if (instrument === 'probe') {
      const child = await runChild({
        sink: context.sink,
        id,
        command: process.execPath,
        args: [probePath],
        env: { ...process.env },
        limitMs,
      });
      return { kind: 'probe', child, probe: parseProbe(child) };
    }
    const env = { ...process.env, WEBTRANSPORT_LIVE: '1' };
    delete env.WT_LIVE_NODE_MODULES;
    const child = await runChild({
      sink: context.sink,
      id,
      command: process.execPath,
      args: [vitestPath, 'run', lockedPath, '--reporter=dot', '--maxWorkers=1', '--pool=forks'],
      env,
      limitMs,
    });
    const combined = `${child.stdoutText}\n${child.stderrText}`;
    return {
      kind: 'locked',
      child,
      targetTimeout: combined.includes('listener baseline after 32 CONNECTs timeout'),
    };
  });
}

function validateMeasuredWindow(arm, window, execution = false) {
  const reasons = [];
  if (!validArmCpu(arm, window.cpu)) reasons.push(`${arm}-cpu-out-of-range`);
  if (!window.server.valid) reasons.push('server-3100-identity-or-connections');
  if (execution && !validServerExecution(window.server))
    reasons.push('server-3100-material-change');
  if (execution) {
    const child = window.value.child;
    if (!child.pipesClosed || child.processAliveAfterClose) reasons.push('child-not-quiescent');
    if (window.value.kind === 'probe' && !window.value.probe.ok) reasons.push('probe-failed');
  }
  return reasons;
}

async function runArm(context, descriptor) {
  const arm = descriptor.arm;
  if (arm === 'A') await context.burners.stop();
  const stabilizationResult =
    arm === 'A'
      ? await stabilizeA(context, descriptor)
      : await stabilizeB(context, descriptor, descriptor.bMin, descriptor.bMax, descriptor.bP95);
  const stabilization = stabilizationResult.window;
  if (!stabilization) {
    if (arm === 'B') await context.burners.stop();
    return {
      ...descriptor,
      valid: false,
      reasons: [`${arm}-stabilization-timeout`],
      stabilization: null,
      stabilizationTimeout: stabilizationTimeoutEvidence(stabilizationResult),
    };
  }
  try {
    const order = descriptor.instrumentOrder;
    const instruments = order === 'PL' ? ['probe', 'locked'] : ['locked', 'probe'];
    const first = await executeInstrument(context, descriptor, instruments[0]);
    const firstReasons = validateMeasuredWindow(arm, first, true);
    const interval = await passiveWindow(context, `${arm}-interval`, QUIET_WINDOW_MS);
    const intervalReasons = validateMeasuredWindow(arm, interval, false);
    const second = await executeInstrument(context, descriptor, instruments[1]);
    const secondReasons = validateMeasuredWindow(arm, second, true);
    const reasons = [
      ...validateMeasuredWindow(arm, stabilization, false),
      ...firstReasons,
      ...intervalReasons,
      ...secondReasons,
    ];
    const probeWindow = first.value.kind === 'probe' ? first : second;
    const lockedWindow = first.value.kind === 'locked' ? first : second;
    return {
      ...descriptor,
      valid: reasons.length === 0,
      reasons,
      stabilization,
      stabilizationCandidates: stabilizationResult.candidates,
      interval,
      first,
      second,
      probeWindow,
      lockedWindow,
    };
  } finally {
    if (arm === 'B') await context.burners.stop();
  }
}

function serverPairDifference(pair) {
  const totals = {};
  for (const arm of ['A', 'B']) {
    const current = pair.arms[arm];
    if (!current?.probeWindow || !current?.lockedWindow)
      return { valid: false, reason: 'arm-incomplete' };
    totals[arm] = {
      cpuMs: current.probeWindow.server.cpuMs + current.lockedWindow.server.cpuMs,
      ioBytes: current.probeWindow.server.ioBytes + current.lockedWindow.server.ioBytes,
    };
  }
  const cpuDifference = Math.abs(totals.A.cpuMs - totals.B.cpuMs);
  const ioDifference = Math.abs(totals.A.ioBytes - totals.B.ioBytes);
  return {
    valid: cpuDifference <= 250 && ioDifference <= 512 * 1024,
    cpuDifference,
    ioDifference,
    totals,
  };
}

async function runPair(context, specification) {
  const before = await candidateSnapshot();
  if (before.manifest !== context.reference.manifest)
    throw new Error('candidate manifest drift before pair');
  const arms = {};
  for (const arm of specification.armOrder) {
    arms[arm] = await runArm(context, {
      id: specification.id,
      pilot: specification.pilot ?? false,
      calibration: specification.calibration ?? false,
      attempt: specification.attempt ?? null,
      armOrder: specification.armOrder,
      arm,
      instrumentOrder: specification.instrumentOrders[arm],
      bMin: specification.bMin ?? 70,
      bMax: specification.bMax ?? 80,
      bP95: specification.bP95 ?? 95,
    });
  }
  const after = await candidateSnapshot();
  if (after.manifest !== context.reference.manifest)
    throw new Error('candidate manifest drift after pair');
  const pair = { ...specification, before, after, arms };
  pair.serverDifference = serverPairDifference(pair);
  pair.valid = arms.A.valid && arms.B.valid && pair.serverDifference.valid;
  pair.reasons = [
    ...arms.A.reasons,
    ...arms.B.reasons,
    ...(pair.serverDifference.valid ? [] : ['server-3100-pair-difference']),
  ];
  await context.sink.append({ type: specification.pilot ? 'pilot' : 'pair', ...pair });
  return pair;
}

function probeValue(armResult) {
  return armResult.probeWindow.value.probe;
}

function lockedValue(armResult) {
  return armResult.lockedWindow.value;
}

function drainInterval(probe) {
  return probe.censored
    ? { lower: 10_000, upper: null }
    : { lower: probe.drainMs, upper: probe.drainMs };
}

function quantileInterval(intervals, fraction) {
  const sorted = [...intervals].sort((left, right) => left.lower - right.lower);
  const rank = Math.max(1, Math.ceil(fraction * sorted.length));
  return sorted[rank - 1];
}

function deltaInterval(b, a) {
  if (b.upper !== null && a.upper !== null)
    return { lower: b.lower - a.lower, upper: b.upper - a.upper };
  if (b.upper === null && a.upper !== null) return { lower: b.lower - a.upper, upper: null };
  if (b.upper !== null && a.upper === null) return { lower: null, upper: b.upper - a.lower };
  return { lower: null, upper: null };
}

function classify(validPairs, calibrations, integrityFailure = false, environmentFailure = false) {
  if (integrityFailure) return { classification: 'aborted-integrity' };
  if (environmentFailure || validPairs.length < REQUIRED_VALID_PAIRS) {
    return { classification: 'inconclusive-environment' };
  }
  const timeoutAndProbe = [];
  const validCalibrations = calibrations.filter((pair) => pair.valid);
  for (const pair of [...validPairs, ...validCalibrations]) {
    for (const arm of ['A', 'B']) {
      const current = pair.arms?.[arm];
      if (!current?.probeWindow || !current?.lockedWindow) continue;
      if (lockedValue(current).targetTimeout)
        timeoutAndProbe.push({ arm, probe: probeValue(current) });
    }
  }
  const divergent = timeoutAndProbe.find(
    ({ probe }) => !probe.censored && Number(probe.drainMs) < 9000,
  );
  if (divergent) return { classification: 'inconclusive-probe', divergent };

  const aIntervals = validPairs.map((pair) => drainInterval(probeValue(pair.arms.A)));
  const bIntervals = validPairs.map((pair) => drainInterval(probeValue(pair.arms.B)));
  const deltas = validPairs.map((pair, index) =>
    deltaInterval(bIntervals[index], aIntervals[index]),
  );
  const aTimeouts = validPairs.filter((pair) => lockedValue(pair.arms.A).targetTimeout).length;
  const bTimeouts = validPairs.filter((pair) => lockedValue(pair.arms.B).targetTimeout).length;
  const calibrationTimeouts = validCalibrations.filter((pair) =>
    Object.values(pair.arms ?? {}).some(
      (arm) => arm?.lockedWindow && lockedValue(arm).targetTimeout,
    ),
  ).length;
  const aP90 = quantileInterval(aIntervals, 0.9);
  const aMax = quantileInterval(aIntervals, 1);
  const bP90 = quantileInterval(bIntervals, 0.9);
  const deltaLower = deltas.map((value) => value.lower ?? Number.NEGATIVE_INFINITY);
  const deltaMedianLower = percentile(deltaLower, 0.5);
  const aFast =
    aIntervals.every((value) => value.upper !== null) &&
    aTimeouts === 0 &&
    aP90.upper <= 5000 &&
    aMax.upper <= 7500;
  const aSlow =
    aIntervals.some((value) => value.upper === null) ||
    aTimeouts > 0 ||
    aP90.lower >= 8000 ||
    aMax.lower >= 9000;
  const controlPositive = timeoutAndProbe.some(
    ({ probe }) => probe.censored || Number(probe.drainMs) >= 9000,
  );
  const bTailCount = bIntervals.filter(
    (value) => value.upper === null || value.lower >= 9000,
  ).length;
  const bTailShift =
    controlPositive &&
    bTailCount >= 2 &&
    bTimeouts >= 1 &&
    bP90.lower >= 9000 &&
    deltaMedianLower >= 2000;
  const evidence = {
    aIntervals,
    bIntervals,
    deltas,
    aP90,
    aMax,
    bP90,
    deltaMedianLower,
    aTimeouts,
    bTimeouts,
    calibrationTimeouts,
    bTailCount,
    controlPositive,
    aFast,
    aSlow,
    bTailShift,
  };
  if (aSlow) return { classification: 'product-suspect', evidence };
  if (aFast && bTailShift) return { classification: 'cpu-causal', evidence };
  if (aFast && bTimeouts === 0 && calibrationTimeouts === 0) {
    return { classification: 'cpu-not-reproduced', evidence };
  }
  return { classification: 'inconclusive-effect', evidence };
}

async function writeArtifactIndex(context, result) {
  const rawFiles = await readdir(path.join(context.output, 'raw'));
  const lines = [
    '---',
    'kind: spec',
    'title: "Sprint 02D — diagnóstico A/B de dreno WebTransport"',
    '---',
    '',
    '# Diagnóstico A/B de dreno WebTransport',
    '',
    `Classificação mecânica: **${result.classification.classification}**.`,
    '',
    `- Início: ${new Date(context.startedAt).toISOString()}`,
    `- Término: ${new Date().toISOString()}`,
    `- Pares válidos: ${result.validPairs.length}/12`,
    `- Tentativas: ${result.attempts.length}/16`,
    `- Manifest: \`${context.reference.manifest}\``,
    `- Locks: 8/8`,
    '',
    'Dados executáveis:',
    '',
    '- [`raw.jsonl`](raw.jsonl)',
    '- [`summary.json`](summary.json)',
    '- [`run-state.json`](run-state.json)',
    ...(rawFiles.length ? ['- [`raw/`](raw/)'] : []),
    '',
    'O probe é apenas cronômetro; o teste locked permaneceu normativo e intacto.',
    '',
  ];
  await context.sink.write('index.md', lines.join('\n'));
}

async function runWorker(output, startedAt, runId) {
  const sink = await SinkGuard.attach(output, runId);
  const context = {
    output,
    sink,
    startedAt,
    measureStopAt: startedAt + MEASURE_STOP_MS,
    cpu: new CpuSampler(),
    burners: new BurnerPool(),
    server: new ServerTelemetry(),
    loopDelay: monitorEventLoopDelay({ resolution: 20 }),
    reference: null,
    serverBaseline: null,
    stabilizationRecords: [],
  };
  const attempts = [];
  const pilots = [];
  const validPairs = [];
  const calibrations = [];
  let terminalWritten = false;

  const terminal = async (status, extra = {}) => {
    if (terminalWritten) return;
    const value = { status: 'terminal', outcome: status, at: new Date().toISOString(), ...extra };
    await sink.append({ type: 'terminal', ...value });
    await sink.writeJson('run-state.json', value);
    terminalWritten = true;
  };

  try {
    const seeds = verifySeeds();
    context.reference = await candidateSnapshot();
    await sink.writeJson('run-state.json', {
      status: 'running',
      startedAt,
      deadline: startedAt + WALL_MS,
      stage: 'worker-start',
      runId,
    });
    await sink.append({ type: 'start', startedAt, seeds, reference: context.reference });
    context.cpu.start();
    context.loopDelay.enable();
    context.serverBaseline = await context.server.start();

    const pilotSpecs = [
      { id: 'pilot-1', pilot: true, armOrder: 'AB', instrumentOrders: { A: 'PL', B: 'LP' } },
      { id: 'pilot-2', pilot: true, armOrder: 'BA', instrumentOrders: { A: 'LP', B: 'PL' } },
    ];
    for (const specification of pilotSpecs) {
      if (Date.now() >= context.measureStopAt) break;
      pilots.push(await runPair(context, specification));
    }

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      if (validPairs.length >= REQUIRED_VALID_PAIRS || Date.now() >= context.measureStopAt) break;
      if (validPairs.length + (MAX_ATTEMPTS - attempt + 1) < REQUIRED_VALID_PAIRS) break;
      const order = armOrder(attempt);
      const pair = await runPair(context, {
        id: `attempt-${String(attempt).padStart(2, '0')}`,
        attempt,
        pilot: false,
        armOrder: order,
        instrumentOrders: { A: instrumentOrder(attempt, 'A'), B: instrumentOrder(attempt, 'B') },
      });
      attempts.push(pair);
      if (pair.valid) validPairs.push(pair);
    }

    if (
      validPairs.length === REQUIRED_VALID_PAIRS &&
      !validPairs.some((pair) => lockedValue(pair.arms.B).targetTimeout)
    ) {
      for (let index = 1; index <= 2 && Date.now() < context.measureStopAt; index++) {
        const calibration = await runPair(context, {
          id: `calibration-${index}`,
          calibration: true,
          armOrder: 'BA',
          instrumentOrders: { A: index === 1 ? 'PL' : 'LP', B: index === 1 ? 'LP' : 'PL' },
          bMin: 85,
          bMax: 90,
          bP95: 98,
        });
        calibrations.push(calibration);
        if (
          Object.values(calibration.arms).some(
            (arm) => arm?.lockedWindow && lockedValue(arm).targetTimeout,
          )
        )
          break;
      }
    }

    const classification = classify(
      validPairs,
      calibrations,
      false,
      validPairs.length < REQUIRED_VALID_PAIRS ||
        Date.now() >= context.measureStopAt ||
        calibrations.some((pair) => !pair.valid),
    );
    const summary = {
      classification,
      startedAt,
      endedAt: Date.now(),
      reference: context.reference,
      pilots,
      attempts,
      validPairs,
      calibrations,
      stabilizationCandidates: context.stabilizationRecords,
      cpu: cpuMetrics(context.cpu.samples),
      serverSamples: context.server.samples,
    };
    await sink.writeJson('summary.json', summary);
    await writeArtifactIndex(context, summary);
    await terminal(classification.classification, {
      classification,
      validPairs: validPairs.length,
      attempts: attempts.length,
      manifest: context.reference.manifest,
      stabilizationCandidates: context.stabilizationRecords,
    });
  } catch (error) {
    try {
      await terminal('instrument-error', {
        error: error?.stack ?? error?.message ?? String(error),
      });
    } catch {
      // The supervisor owns the independent terminal. A lost sink must never be recreated here.
    }
    throw error;
  } finally {
    await context.burners.stop();
    await context.server.stop();
    context.cpu.stop();
    context.loopDelay.disable();
  }
}

async function treeManifest(rootPath) {
  const entries = [];
  async function visit(directory) {
    const children = await readdir(directory, { withFileTypes: true });
    children.sort((left, right) => left.name.localeCompare(right.name));
    for (const child of children) {
      const file = path.join(directory, child.name);
      if (child.isDirectory()) await visit(file);
      else if (child.isFile()) {
        const value = await readFile(file);
        entries.push({
          file: path.relative(rootPath, file).replaceAll('\\', '/'),
          bytes: value.length,
          sha256: sha256(value),
        });
      } else {
        throw new Error(`instrument-error: unsupported sink entry ${file}`);
      }
    }
  }
  await visit(rootPath);
  return entries;
}

function sameManifest(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

async function validateSuccessfulSink(spool, runId) {
  const sink = await SinkGuard.attach(spool, runId);
  await sink.assertJsonl();
  const jsonl = await parseJsonl(path.join(spool, 'raw.jsonl'));
  if (jsonl.terminalCount !== 1 || jsonl.records.at(-1)?.type !== 'terminal') {
    throw new Error('instrument-error: terminal JSONL count/order invalid');
  }
  const [state, summary] = await Promise.all([
    readFile(path.join(spool, 'run-state.json'), 'utf8').then(JSON.parse),
    readFile(path.join(spool, 'summary.json'), 'utf8').then(JSON.parse),
  ]);
  if (state.status !== 'terminal' || state.outcome === 'instrument-error') {
    throw new Error('instrument-error: worker terminal is not publishable');
  }
  if (!(await pathExists(path.join(spool, 'index.md')))) {
    throw new Error('instrument-error: result index missing');
  }
  const start = jsonl.records[0];
  const terminal = jsonl.records.at(-1);
  const manifest = summary.reference?.manifest;
  if (
    start?.type !== 'start' ||
    !manifest ||
    start.reference?.manifest !== manifest ||
    terminal.manifest !== manifest ||
    state.manifest !== manifest ||
    summary.classification?.classification !== state.outcome
  ) {
    throw new Error('instrument-error: result manifest/terminal mismatch');
  }
  return { sink, jsonl, state, summary, manifest };
}

async function ensureVacant(file) {
  if (await pathExists(file))
    throw new Error(`instrument-error: publication target exists: ${file}`);
  if (!(await pathExists(path.dirname(file)))) {
    throw new Error(`instrument-error: publication parent missing: ${path.dirname(file)}`);
  }
}

async function atomicPublishDirectory(stage, target) {
  await ensureVacant(target);
  const expected = await treeManifest(stage);
  await rename(stage, target);
  const observed = await treeManifest(target);
  if (!sameManifest(expected, observed)) {
    throw new Error('instrument-error: published tree hash mismatch');
  }
  return observed;
}

async function publishSuccessfulResult({ spool, publishOutput, runId, supervisorTerminal }) {
  const validated = await validateSuccessfulSink(spool, runId);
  const sourceManifest = await treeManifest(spool);
  const stageParent = await mkdtemp(path.join(tmpdir(), 'discord-locutor-02d-stage-'));
  const stage = path.join(stageParent, 'evidence');
  try {
    await cp(spool, stage, { recursive: true, force: false, errorOnExist: true });
    const copiedManifest = await treeManifest(stage);
    if (!sameManifest(sourceManifest, copiedManifest)) {
      throw new Error('instrument-error: staging tree hash mismatch');
    }
    await atomicStandaloneJson(path.join(stage, 'supervisor-terminal.json'), supervisorTerminal);
    await atomicStandaloneJson(path.join(stage, 'publication-verification.json'), {
      schemaVersion: 1,
      runId,
      sourceManifest,
      jsonlRecords: validated.jsonl.count,
      jsonlTerminalCount: validated.jsonl.terminalCount,
      outcome: validated.state.outcome,
      candidateManifest: validated.manifest,
      verifiedAt: new Date().toISOString(),
    });
    const publishedManifest = await atomicPublishDirectory(stage, publishOutput);
    return { stageParent, publishedManifest, outcome: validated.state.outcome };
  } catch (error) {
    await rm(stageParent, { recursive: true, force: true });
    throw error;
  }
}

async function publishFailureDiagnostic({ publishOutput, supervisorTerminal }) {
  const stageParent = await mkdtemp(path.join(tmpdir(), 'discord-locutor-02d-failure-stage-'));
  const stage = path.join(stageParent, 'evidence');
  await mkdir(stage, { recursive: false });
  try {
    const failureState = {
      status: 'terminal',
      outcome: 'instrument-error',
      at: supervisorTerminal.at,
      runId: supervisorTerminal.runId,
      reason: supervisorTerminal.reason,
    };
    await atomicStandaloneJson(path.join(stage, 'run-state.json'), failureState);
    await atomicStandaloneJson(path.join(stage, 'supervisor-terminal.json'), supervisorTerminal);
    await writeFile(
      path.join(stage, 'index.md'),
      [
        '---',
        'kind: review',
        'title: "Sprint 02D — falha fechada do instrumento"',
        '---',
        '',
        '# Falha fechada do instrumento',
        '',
        'A execução perdeu ou não conseguiu verificar seu sink. Nenhum resultado A/B foi publicado.',
        '',
        `Motivo: \`${String(supervisorTerminal.reason).replaceAll('`', '')}\``,
        '',
      ].join('\n'),
    );
    await atomicStandaloneJson(path.join(stage, 'publication-verification.json'), {
      schemaVersion: 1,
      runId: supervisorTerminal.runId,
      outcome: 'instrument-error',
      resultPublished: false,
      verifiedAt: new Date().toISOString(),
    });
    const publishedManifest = await atomicPublishDirectory(stage, publishOutput);
    return { stageParent, publishedManifest, outcome: 'instrument-error' };
  } catch (error) {
    await rm(stageParent, { recursive: true, force: true });
    throw error;
  }
}

async function removeAndAssert(paths) {
  for (const target of paths) await rm(target, { recursive: true, force: true });
  for (const target of paths) {
    if (await pathExists(target)) throw new Error(`instrument-error: cleanup residual ${target}`);
  }
}

async function runSupervisor(output) {
  if (!output) throw new Error('--output is required');
  await ensureVacant(output);
  const startedAt = Date.now();
  const runId = randomUUID();
  const spool = await mkdtemp(path.join(tmpdir(), `discord-locutor-02d-${startedAt}-`));
  const terminalDirectory = await mkdtemp(
    path.join(tmpdir(), `discord-locutor-02d-terminal-${startedAt}-`),
  );
  const terminalFile = path.join(terminalDirectory, 'supervisor-terminal.json');
  await SinkGuard.create(spool, runId);
  process.stdout.write(`DRAIN_AB_SPOOL ${JSON.stringify({ runId, spool, terminalFile })}\n`);

  let childResult = { code: null, signal: null, error: null };
  let wallTimedOut = false;
  let workerState = null;
  let supervisorTerminal;
  try {
    const child = spawn(
      process.execPath,
      [
        selfPath,
        '--worker',
        '--output',
        spool,
        '--started-at',
        String(startedAt),
        '--run-id',
        runId,
      ],
      { cwd: root, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true },
    );
    child.stdout.pipe(process.stdout);
    child.stderr.pipe(process.stderr);
    const timer = setTimeout(() => {
      wallTimedOut = true;
      terminateTree(child);
    }, WALL_MS);
    childResult = await new Promise((resolve) => {
      child.once('error', (error) => resolve({ code: null, signal: null, error }));
      child.once('close', (code, signal) => resolve({ code, signal, error: null }));
    });
    clearTimeout(timer);
    try {
      const guard = await SinkGuard.attach(spool, runId);
      await guard.assertJsonl();
      workerState = JSON.parse(await readFile(path.join(spool, 'run-state.json'), 'utf8'));
    } catch (error) {
      workerState = { status: 'unavailable', error: error?.message ?? String(error) };
    }
  } catch (error) {
    childResult = { code: null, signal: null, error };
  } finally {
    const successfulWorker =
      childResult.code === 0 &&
      !wallTimedOut &&
      workerState?.status === 'terminal' &&
      workerState.outcome !== 'instrument-error';
    const reason = successfulWorker
      ? null
      : wallTimedOut
        ? 'wall-timeout'
        : workerState?.error ||
          childResult.error?.stack ||
          childResult.error?.message ||
          workerState?.outcome ||
          `worker-exit-${childResult.code}`;
    supervisorTerminal = {
      schemaVersion: 1,
      status: 'terminal',
      outcome: successfulWorker ? workerState.outcome : 'instrument-error',
      at: new Date().toISOString(),
      runId,
      startedAt,
      wallTimedOut,
      reason,
      workerState,
      child: {
        code: childResult.code,
        signal: childResult.signal,
        error: childResult.error?.stack ?? childResult.error?.message ?? null,
      },
    };
    await atomicStandaloneJson(terminalFile, supervisorTerminal);
  }

  let publication;
  try {
    if (supervisorTerminal.outcome === 'instrument-error') {
      publication = await publishFailureDiagnostic({ publishOutput: output, supervisorTerminal });
    } else {
      publication = await publishSuccessfulResult({
        spool,
        publishOutput: output,
        runId,
        supervisorTerminal,
      });
    }
    await removeAndAssert([spool, terminalDirectory, publication.stageParent]);
  } catch (error) {
    const failedTerminal = {
      ...supervisorTerminal,
      outcome: 'instrument-error',
      publicationError: error?.stack ?? error?.message ?? String(error),
    };
    await atomicStandaloneJson(
      path.join(terminalDirectory, 'supervisor-publication-error.json'),
      failedTerminal,
    );
    if (!(await pathExists(output))) {
      publication = await publishFailureDiagnostic({
        publishOutput: output,
        supervisorTerminal: failedTerminal,
      });
      await removeAndAssert([spool, terminalDirectory, publication.stageParent]);
    }
    process.stdout.write(`DRAIN_AB_TERMINAL ${JSON.stringify(failedTerminal)}\n`);
    process.exitCode = 1;
    return;
  }

  process.stdout.write(`DRAIN_AB_TERMINAL ${JSON.stringify(supervisorTerminal)}\n`);
  if (supervisorTerminal.outcome === 'instrument-error') process.exitCode = 1;
}

async function createHealthyCanarySink(reference) {
  const runId = randomUUID();
  const spool = await mkdtemp(path.join(tmpdir(), 'discord-locutor-02d-healthy-canary-'));
  const sink = await SinkGuard.create(spool, runId);
  const classification = { classification: 'inconclusive-environment' };
  await sink.append({ type: 'start', reference });
  await sink.writeJson('summary.json', { classification, reference });
  await sink.write(
    'index.md',
    ['---', 'kind: spec', 'title: "healthy canary"', '---', '', '# Healthy canary', ''].join('\n'),
  );
  const terminal = {
    status: 'terminal',
    outcome: classification.classification,
    at: new Date().toISOString(),
    manifest: reference.manifest,
  };
  await sink.append({ type: 'terminal', ...terminal });
  await sink.writeJson('run-state.json', terminal);
  return { runId, spool };
}

async function runSinkCanaryWorker(output, runId) {
  const sink = await SinkGuard.attach(output, runId);
  await sink.append({ type: 'start', canary: true });
  process.stdout.write('SINK_CANARY_READY\n');
  await new Promise((resolve, reject) => {
    process.stdin.once('data', resolve);
    process.stdin.once('error', reject);
    process.stdin.resume();
  });
  await sink.append({ type: 'pair', forbidden: true });
  throw new Error('sink-loss canary worker continued after primary sink removal');
}

async function runSinkLossWorkerCanary() {
  const runId = randomUUID();
  const spool = await mkdtemp(path.join(tmpdir(), 'discord-locutor-02d-lost-canary-'));
  await SinkGuard.create(spool, runId);
  const redundantDirectory = await mkdtemp(
    path.join(tmpdir(), 'discord-locutor-02d-redundant-canary-'),
  );
  const redundantTerminal = path.join(redundantDirectory, 'supervisor-terminal.json');
  const stdout = [];
  const stderr = [];
  const child = spawn(
    process.execPath,
    [selfPath, '--sink-canary-worker', '--output', spool, '--run-id', runId],
    { cwd: root, stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true },
  );
  child.stdout.on('data', (chunk) => stdout.push(Buffer.from(chunk)));
  child.stderr.on('data', (chunk) => stderr.push(Buffer.from(chunk)));
  const closePromise = new Promise((resolve) => {
    child.once('close', (code, signal) => resolve({ code, signal }));
  });
  let result;
  let sinkLossError;
  try {
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        terminateTree(child);
        reject(new Error('sink-loss canary worker ready timeout'));
      }, 10_000);
      const inspect = () => {
        if (Buffer.concat(stdout).toString('utf8').includes('SINK_CANARY_READY')) {
          clearTimeout(timeout);
          resolve();
        }
      };
      child.stdout.on('data', inspect);
      child.once('error', (error) => {
        clearTimeout(timeout);
        reject(error);
      });
      child.once('close', (code) => {
        if (!Buffer.concat(stdout).toString('utf8').includes('SINK_CANARY_READY')) {
          clearTimeout(timeout);
          reject(new Error(`sink-loss canary worker exited before ready: ${code}`));
        }
      });
      inspect();
    });
    await new Promise((resolve) => setTimeout(resolve, 5000));
    const survivedWait = (await readFile(path.join(spool, 'raw.jsonl'), 'utf8')).includes(
      '"recordSequence":1',
    );
    if (!survivedWait) throw new Error('sink durability wait self-check failed');
    await rm(spool, { recursive: true, force: true });
    child.stdin.end('continue\n');
    result = await closePromise;
    const combined = `${Buffer.concat(stdout).toString('utf8')}\n${Buffer.concat(stderr).toString('utf8')}`;
    if (result.code !== 1 || !combined.includes(SINK_ERROR)) {
      throw new Error(
        `sink-loss canary worker did not fail nominally: ${JSON.stringify({ result, combined })}`,
      );
    }
    sinkLossError = SINK_ERROR;
    return { runId, spool, redundantDirectory, redundantTerminal, survivedWait, result };
  } finally {
    if (processIsAlive(child.pid)) terminateTree(child);
    await atomicStandaloneJson(redundantTerminal, {
      status: 'terminal',
      outcome: 'instrument-error',
      reason: sinkLossError ?? 'sink-loss-canary-worker-failed-before-nominal-error',
      runId,
      child: result ?? null,
    });
  }
}

async function mutationCanary(reference, name, mutate) {
  const runId = randomUUID();
  const spool = await mkdtemp(path.join(tmpdir(), `discord-locutor-02d-${name}-canary-`));
  const sink = await SinkGuard.create(spool, runId);
  await sink.append({ type: 'start', reference });
  await mutate(spool, runId);
  let observed;
  try {
    await sink.append({ type: 'pair', forbidden: true });
  } catch (error) {
    observed = error;
  }
  await rm(spool, { recursive: true, force: true });
  if (observed?.code !== 'SINK_LOST_OR_REPLACED') {
    throw new Error(`${name} did not fail closed`);
  }
  return true;
}

async function stabilizationRetentionCanary() {
  const runId = randomUUID();
  const spool = await mkdtemp(
    path.join(tmpdir(), 'discord-locutor-02d-stabilization-retention-canary-'),
  );
  const sink = await SinkGuard.create(spool, runId);
  const context = { sink, stabilizationRecords: [] };
  const syntheticCpu = (busyPercent, start) =>
    cpuMetrics(
      Array.from({ length: 60 }, (_, index) => ({
        at: start + index * SAMPLE_MS,
        busyPercent,
        freeMemory: 1,
        totalMemory: 1,
        rss: 1,
      })),
    );
  const baseWindow = {
    label: 'stabilization-retention-canary',
    start: 1000,
    end: 16_000,
    durationMs: 15_000,
    server: {
      valid: true,
      identityValid: true,
      established: [0],
      cpuMs: 0,
      ioBytes: 0,
      count: 15,
    },
    eventLoop: { minMs: 1, maxMs: 4, p50Ms: 1, p95Ms: 3, p99Ms: 4 },
    burners: {
      start: { count: 0, aliveCount: 0, children: [], dutyCycle: null },
      end: { count: 0, aliveCount: 0, children: [], dutyCycle: null },
    },
  };
  const aWindow = {
    ...baseWindow,
    cpu: syntheticCpu(30, 1000),
  };
  const bWindow = {
    ...baseWindow,
    start: 17_000,
    end: 32_000,
    cpu: syntheticCpu(40, 17_000),
    burners: {
      start: {
        count: 3,
        aliveCount: 3,
        children: [{ pid: 1 }, { pid: 2 }, { pid: 3 }],
        dutyCycle: { workMs: 25, yield: 'setImmediate', fraction: 1 },
      },
      end: {
        count: 3,
        aliveCount: 3,
        children: [{ pid: 1 }, { pid: 2 }, { pid: 3 }],
        dutyCycle: { workMs: 25, yield: 'setImmediate', fraction: 1 },
      },
    },
  };
  const aDescriptor = {
    id: 'canary-pair',
    pilot: false,
    calibration: false,
    attempt: 1,
    arm: 'A',
    armOrder: 'AB',
    instrumentOrder: 'PL',
  };
  const bDescriptor = { ...aDescriptor, arm: 'B', instrumentOrder: 'LP' };
  const aReference = await persistStabilizationCandidate(
    context,
    aDescriptor,
    1,
    aWindow,
    evaluateStabilizationCpu('A', aWindow.cpu),
  );
  const bReference = await persistStabilizationCandidate(
    context,
    bDescriptor,
    1,
    bWindow,
    evaluateStabilizationCpu('B', bWindow.cpu),
  );
  const terminal = {
    type: 'terminal',
    status: 'terminal',
    outcome: 'inconclusive-environment',
    stabilizationCandidates: context.stabilizationRecords,
    stabilizationTimeout: stabilizationTimeoutEvidence({
      candidates: [aReference, bReference],
      deadline: 33_000,
    }),
  };
  await sink.append(terminal);
  await sink.writeJson('run-state.json', terminal);
  const parsed = await parseJsonl(path.join(spool, 'raw.jsonl'));
  const candidates = parsed.records.filter((record) => record.type === 'stabilization-candidate');
  const terminalRecord = parsed.records.at(-1);
  const state = JSON.parse(await readFile(path.join(spool, 'run-state.json'), 'utf8'));
  const verified =
    candidates.length === 2 &&
    candidates.every(
      (record) =>
        record.accepted === false &&
        record.window?.start &&
        record.window?.end &&
        record.window?.cpu?.atOrBelowRuns &&
        record.window?.server &&
        record.window?.eventLoop &&
        record.window?.burners &&
        record.limits &&
        record.checks,
    ) &&
    JSON.stringify(terminalRecord.stabilizationCandidates) ===
      JSON.stringify([aReference, bReference]) &&
    JSON.stringify(state.stabilizationCandidates) === JSON.stringify([aReference, bReference]) &&
    JSON.stringify(terminalRecord.stabilizationTimeout.recordSequences) ===
      JSON.stringify([1, 2]) &&
    JSON.stringify(state.stabilizationTimeout.recordSequences) === JSON.stringify([1, 2]);
  await removeAndAssert([spool]);
  if (!verified) throw new Error('stabilization retention canary failed');
  return { verified, recordSequences: [aReference.recordSequence, bReference.recordSequence] };
}

function continuityCanary() {
  const sample = (at, busyPercent) => ({
    at,
    busyPercent,
    freeMemory: 1,
    totalMemory: 1,
    rss: 1,
  });
  const breach = longestRunAtOrBelow(
    [sample(0, 10), sample(SAMPLE_MS, 100), sample(SAMPLE_MS * 2, 10)],
    20,
  );
  const missingSampleGap = longestRunAtOrBelow([sample(0, 10), sample(SAMPLE_MS * 2, 10)], 20);
  const breachSplits =
    breach.coveredMsEstimate === SAMPLE_MS && breach.sampleCount === 1 && breach.start === 0;
  const missingSampleGapJoins =
    missingSampleGap.coveredMsEstimate === SAMPLE_MS * 3 &&
    missingSampleGap.sampleCount === 2 &&
    missingSampleGap.start === 0 &&
    missingSampleGap.end === SAMPLE_MS * 2;
  if (!breachSplits) {
    throw new Error(`continuity breach canary failed: ${JSON.stringify(breach)}`);
  }
  if (!missingSampleGapJoins) {
    throw new Error(
      `continuity missing-sample gap canary failed: ${JSON.stringify(missingSampleGap)}`,
    );
  }
  return { breachSplits, missingSampleGapJoins, breach, missingSampleGap };
}

async function runSelfCheck() {
  const seeds = verifySeeds();
  const reference = await candidateSnapshot();
  const server = new ServerTelemetry();
  const baseline = await server.start();
  await new Promise((resolve) => setTimeout(resolve, 2200));
  await server.stop();
  if (server.samples.length < 2) throw new Error('server telemetry self-check insufficient');

  const sinkLoss = await runSinkLossWorkerCanary();
  const sinkLossFailClosed = sinkLoss.result.code === 1;
  const redundantTerminalReadable =
    JSON.parse(await readFile(sinkLoss.redundantTerminal, 'utf8')).outcome === 'instrument-error';
  const sentinelReplacementFailClosed = await mutationCanary(
    reference,
    'sentinel-replacement',
    async (spool, runId) => {
      await writeFile(
        path.join(spool, SINK_SENTINEL),
        `${JSON.stringify({ schemaVersion: 1, runId, replaced: true })}\n`,
      );
    },
  );
  const jsonlTruncationFailClosed = await mutationCanary(
    reference,
    'jsonl-truncation',
    async (spool) => writeFile(path.join(spool, 'raw.jsonl'), ''),
  );

  const failureParent = await mkdtemp(path.join(tmpdir(), 'discord-locutor-02d-failure-parent-'));
  const failureOutput = path.join(failureParent, 'published');
  const failurePublication = await publishFailureDiagnostic({
    publishOutput: failureOutput,
    supervisorTerminal: JSON.parse(await readFile(sinkLoss.redundantTerminal, 'utf8')),
  });
  const failureState = JSON.parse(
    await readFile(path.join(failureOutput, 'run-state.json'), 'utf8'),
  );
  const classificationAbsent =
    !(await pathExists(path.join(failureOutput, 'summary.json'))) &&
    !Object.hasOwn(failureState, 'classification') &&
    failureState.outcome === 'instrument-error';

  const healthy = await createHealthyCanarySink(reference);
  const healthyParent = await mkdtemp(path.join(tmpdir(), 'discord-locutor-02d-healthy-parent-'));
  const healthyOutput = path.join(healthyParent, 'published');
  const healthyTerminal = {
    schemaVersion: 1,
    status: 'terminal',
    outcome: 'inconclusive-environment',
    at: new Date().toISOString(),
    runId: healthy.runId,
  };
  const healthyPublication = await publishSuccessfulResult({
    spool: healthy.spool,
    publishOutput: healthyOutput,
    runId: healthy.runId,
    supervisorTerminal: healthyTerminal,
  });
  const atomicPublishVerified = (await treeManifest(healthyOutput)).some(
    (entry) => entry.file === 'publication-verification.json',
  );
  const stabilizationRetention = await stabilizationRetentionCanary();
  const continuity = continuityCanary();

  if (!sinkLossFailClosed) throw new Error('sink loss did not fail closed');
  if (!sentinelReplacementFailClosed) throw new Error('sentinel replacement did not fail closed');
  if (!jsonlTruncationFailClosed) throw new Error('JSONL truncation did not fail closed');
  if (!classificationAbsent) throw new Error('sink loss published a classification');
  if (!redundantTerminalReadable) throw new Error('redundant terminal was not readable');
  if (!atomicPublishVerified) throw new Error('atomic publication canary failed');
  if (!stabilizationRetention.verified) throw new Error('stabilization retention canary failed');
  if (!continuity.breachSplits || !continuity.missingSampleGapJoins) {
    throw new Error('continuity canary failed');
  }

  await removeAndAssert([
    sinkLoss.redundantDirectory,
    failureOutput,
    failureParent,
    failurePublication.stageParent,
    healthy.spool,
    healthyOutput,
    healthyParent,
    healthyPublication.stageParent,
  ]);
  const zeroResidual = true;
  process.stdout.write(
    `${JSON.stringify({ ok: true, seeds, manifest: reference.manifest, locks: 8, baseline, serverSamples: server.samples.length, survivedWait: sinkLoss.survivedWait, sinkLossWorkerExit: sinkLoss.result.code, sinkLossFailClosed, sentinelReplacementFailClosed, jsonlTruncationFailClosed, classificationAbsent, redundantTerminalReadable, atomicPublishVerified, stabilizationRetentionVerified: stabilizationRetention.verified, stabilizationRecordSequences: stabilizationRetention.recordSequences, continuityBreachSplits: continuity.breachSplits, continuityMissingSampleGapJoins: continuity.missingSampleGapJoins, continuityBreach: continuity.breach, continuityMissingSampleGap: continuity.missingSampleGap, zeroResidual })}\n`,
  );
}

const options = parseArgs();
if (options.mode === 'burner') runBurner();
else if (options.mode === 'self-check') await runSelfCheck();
else if (options.mode === 'sink-canary-worker') {
  try {
    await runSinkCanaryWorker(options.output, options.runId);
  } catch (error) {
    process.stderr.write(`${error?.stack ?? error}\n`);
    process.exitCode = 1;
  }
} else if (options.mode === 'worker') {
  try {
    await runWorker(options.output, options.startedAt || Date.now(), options.runId);
  } catch (error) {
    process.stderr.write(`${error?.stack ?? error}\n`);
    process.exitCode = 1;
  }
} else {
  await runSupervisor(options.output);
}
