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
import { availableParallelism, cpus, freemem, tmpdir, totalmem } from 'node:os';
import path from 'node:path';
import { monitorEventLoopDelay, performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const selfPath = fileURLToPath(import.meta.url);
const probePath = path.join(root, 'scripts', 'webtransport-drain-probe.mjs');
const vitestPath = path.join(root, 'node_modules', 'vitest', 'vitest.mjs');
const lockedPath = 'server/webtransport-stuck-close.webtransport-independent.test.js';
const SAMPLE_MS = 250;
const SERVER_SAMPLE_MS = 1000;
const WALL_MS = 60 * 60_000;
const MEASURE_STOP_MS = 55 * 60_000;
const UNIT_LIMIT_MS = 80_000;
const BASELINE_MS = 10_000;
const WASH_IN_MS = 5_000;
const PRE_INSTRUMENT_MS = 10_000;
const QUIESCENCE_MS = 500;
const CHILD_CONTRACT_MS = { probe: 50_000, locked: 65_000 };
const SERVER_MAX_GAP_MS = SERVER_SAMPLE_MS * 2;
const SERVER_EDGE_FRESHNESS_MS = Math.ceil(SERVER_SAMPLE_MS * 1.5);
const SERVER_MIN_COVERAGE = 0.8;
const SERVER_MIN_WINDOW_MS = SERVER_SAMPLE_MS * 2;
const BLOCK_COUNT = 12;
const TAU_MS = 10_000;
const SCHEDULE_SEED = 'sprint-02e-20260827-1';
const EXPECTED_BLOCK_ORDER = [
  'L0H',
  '0LH',
  'LH0',
  '0LH',
  'HL0',
  'HL0',
  'L0H',
  'H0L',
  'H0L',
  'LH0',
  '0HL',
  '0HL',
];
const EXPECTED_INSTRUMENT_ORDERS = {
  zero: ['LP', 'PL', 'LP', 'PL', 'PL', 'PL', 'PL', 'LP', 'LP', 'LP', 'LP', 'PL'],
  low: ['PL', 'PL', 'PL', 'LP', 'PL', 'PL', 'LP', 'LP', 'LP', 'LP', 'PL', 'LP'],
  high: ['PL', 'PL', 'PL', 'LP', 'LP', 'PL', 'LP', 'LP', 'PL', 'LP', 'PL', 'LP'],
};
const LEVEL_SYMBOL = { zero: '0', low: 'L', high: 'H' };
const SYMBOL_LEVEL = { 0: 'zero', L: 'low', H: 'high' };
const LOCKED_TEST_TITLE =
  'abre 32 CONNECTs autenticados reais, volta ao baseline e seleciona WT na factory no mesmo listener';
const FROZEN_HASHES = new Map([
  [
    'scripts/webtransport-drain-ab.mjs',
    '06bbc767fe57e9a3118b22fc66f1ed65c61c4b674bdbc71ba490b0769e5e1b43',
  ],
  [
    'scripts/test-webtransport.mjs',
    '3d71af945b1b6557af28afc727fbcca448deb706b8db4b9918be07aa80d8f71d',
  ],
  [
    'scripts/webtransport-drain-probe.mjs',
    'b98deb91cedbec56b95f038644a4ad88f45c67f659d47cfbfe64f432a85f814f',
  ],
]);
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

const CONTROL_GENERATION = 0;
const CONTROL_ACTIVE_COUNT = 1;
const CONTROL_SHUTDOWN = 2;

function runRegimeWorker() {
  const control = new Int32Array(workerData.control);
  const workUnits = new Int32Array(workerData.workUnits);
  const index = workerData.index;
  const buffer = Buffer.alloc(1024 * 1024, index & 0xff);
  let generation = -1;
  parentPort.postMessage({ type: 'ready', index });
  const cycle = () => {
    if (Atomics.load(control, CONTROL_SHUTDOWN)) {
      parentPort.postMessage({ type: 'stopped', index, generation });
      return;
    }
    const observed = Atomics.load(control, CONTROL_GENERATION);
    if (observed !== generation) {
      generation = observed;
      parentPort.postMessage({
        type: 'ack',
        index,
        generation,
        active: index < Atomics.load(control, CONTROL_ACTIVE_COUNT),
      });
    }
    if (index < Atomics.load(control, CONTROL_ACTIVE_COUNT)) {
      const deadline = performance.now() + 25;
      while (performance.now() < deadline) {
        createHash('sha256').update(buffer).digest();
        Atomics.add(workUnits, index, 1);
      }
      setImmediate(cycle);
    } else {
      Atomics.wait(control, CONTROL_GENERATION, generation, 1000);
      setImmediate(cycle);
    }
  };
  cycle();
}

class RegimeWorkers {
  constructor({ high, low }) {
    this.high = high;
    this.low = low;
    this.controlBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * 3);
    this.workBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * high);
    this.control = new Int32Array(this.controlBuffer);
    this.workUnits = new Int32Array(this.workBuffer);
    this.workers = [];
    this.events = [];
    this.failures = [];
    this.pendingAcks = new Map();
    this.generation = 0;
  }

  async start() {
    const ready = [];
    for (let index = 0; index < this.high; index++) {
      let resolveReady;
      const readyPromise = new Promise((resolve) => (resolveReady = resolve));
      ready.push(readyPromise);
      const worker = new Worker(selfPath, {
        workerData: {
          mode: 'regime-worker',
          index,
          control: this.controlBuffer,
          workUnits: this.workBuffer,
        },
      });
      const entry = { index, worker, ready: false, stopped: false, error: null, exitCode: null };
      worker.on('message', (message) => {
        this.events.push({ at: Date.now(), threadId: worker.threadId, ...message });
        if (message.type === 'ready') {
          entry.ready = true;
          resolveReady();
        } else if (message.type === 'ack') {
          this.pendingAcks.get(message.generation)?.add(message.index);
        } else if (message.type === 'stopped') {
          entry.stopped = true;
        }
      });
      worker.on('error', (error) => {
        entry.error = error?.stack ?? error?.message ?? String(error);
        this.failures.push({ index, type: 'error', error: entry.error });
        resolveReady();
      });
      worker.on('exit', (code) => {
        entry.exitCode = code;
        if (!Atomics.load(this.control, CONTROL_SHUTDOWN)) {
          this.failures.push({ index, type: 'unexpected-exit', code });
        }
        resolveReady();
      });
      this.workers.push(entry);
    }
    await Promise.race([
      Promise.all(ready),
      new Promise((_, reject) =>
        setTimeout(
          () => reject(new Error('instrument-error: regime-workers-ready-timeout')),
          10_000,
        ),
      ),
    ]);
    this.assertHealthy();
    await this.setLevel('zero');
  }

  countFor(level) {
    if (level === 'zero') return 0;
    if (level === 'low') return this.low;
    if (level === 'high') return this.high;
    throw new Error(`instrument-error: unknown regime ${level}`);
  }

  assertHealthy() {
    if (
      this.failures.length ||
      this.workers.some((entry) => !entry.ready || entry.exitCode !== null)
    ) {
      throw new Error(`instrument-error: regime-worker-failure ${JSON.stringify(this.failures)}`);
    }
  }

  async setLevel(level) {
    this.assertHealthy();
    const activeCount = this.countFor(level);
    this.generation += 1;
    const acks = new Set();
    this.pendingAcks.set(this.generation, acks);
    Atomics.store(this.control, CONTROL_ACTIVE_COUNT, activeCount);
    Atomics.store(this.control, CONTROL_GENERATION, this.generation);
    Atomics.notify(this.control, CONTROL_GENERATION);
    const deadline = Date.now() + 5000;
    while (acks.size !== this.high && Date.now() < deadline) {
      this.assertHealthy();
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    this.pendingAcks.delete(this.generation);
    if (acks.size !== this.high) {
      throw new Error(`instrument-error: regime-ack-timeout ${level} ${acks.size}/${this.high}`);
    }
    return this.snapshot(level);
  }

  snapshot(level = null) {
    const assigned =
      level === null ? Atomics.load(this.control, CONTROL_ACTIVE_COUNT) : this.countFor(level);
    return {
      pid: process.pid,
      generation: this.generation,
      assigned,
      high: this.high,
      low: this.low,
      failures: [...this.failures],
      workers: this.workers.map((entry, index) => ({
        index,
        threadId: entry.worker.threadId,
        alive: entry.exitCode === null && !entry.error,
        active: index < assigned,
        workUnits: Atomics.load(this.workUnits, index),
        exitCode: entry.exitCode,
        error: entry.error,
      })),
    };
  }

  adherence(start, end, level) {
    this.assertHealthy();
    const expected = this.countFor(level);
    const byIndex = new Map(end.workers.map((worker) => [worker.index, worker]));
    const checks = start.workers.map((before) => {
      const after = byIndex.get(before.index);
      const delta = after.workUnits - before.workUnits;
      const shouldWork = before.index < expected;
      return {
        index: before.index,
        shouldWork,
        delta,
        pass: after.alive && (shouldWork ? delta > 0 : delta === 0),
      };
    });
    return { expected, checks, pass: checks.every((check) => check.pass) };
  }

  async quiesce() {
    const ack = await this.setLevel('zero');
    const before = this.snapshot('zero');
    await new Promise((resolve) => setTimeout(resolve, QUIESCENCE_MS));
    const after = this.snapshot('zero');
    const adherence = this.adherence(before, after, 'zero');
    if (!adherence.pass) throw new Error('instrument-error: regime-workers-not-quiescent');
    return { ack, before, after, adherence };
  }

  async stop() {
    Atomics.store(this.control, CONTROL_ACTIVE_COUNT, 0);
    Atomics.store(this.control, CONTROL_SHUTDOWN, 1);
    Atomics.add(this.control, CONTROL_GENERATION, 1);
    Atomics.notify(this.control, CONTROL_GENERATION);
    await Promise.all(
      this.workers.map(async (entry) => {
        if (entry.exitCode === null) {
          await Promise.race([
            new Promise((resolve) => entry.worker.once('exit', resolve)),
            new Promise((resolve) => setTimeout(resolve, 3000)),
          ]);
        }
        if (entry.worker.threadId !== -1) await entry.worker.terminate();
      }),
    );
  }
}

function appTargetFromEnvironment(environment = process.env) {
  const pid = Number(environment.DRAIN_APP_PID);
  const port = Number(environment.DRAIN_APP_PORT ?? environment.PORT);
  if (Number.isInteger(pid) && pid > 0) return { kind: 'pid', value: pid };
  if (Number.isInteger(port) && port > 0 && port <= 65_535) return { kind: 'port', value: port };
  return { kind: 'discover-node-server', value: null };
}

function serverTelemetryScript(target) {
  const targetPid = target.kind === 'pid' ? target.value : 0;
  const targetPort = target.kind === 'port' ? target.value : 0;
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
$targetPid = ${targetPid}
$targetPort = ${targetPort}
if ($targetPid -gt 0) {
  $owner = [int]$targetPid
} elseif ($targetPort -gt 0) {
  $listeners = @(Get-NetTCPConnection -State Listen -LocalPort $targetPort -ErrorAction SilentlyContinue)
  if ($listeners.Count -ne 1) { throw "app target port $targetPort is absent or ambiguous" }
  $owner = [int]$listeners[0].OwningProcess
} else {
  $serverPids = @(Get-CimInstance Win32_Process -Filter "Name = 'node.exe'" |
    Where-Object { $_.CommandLine -match 'server[\\/]index\.js' } |
    ForEach-Object { [int]$_.ProcessId } | Sort-Object -Unique)
  $owners = @(Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
    Where-Object { $serverPids -contains [int]$_.OwningProcess } |
    ForEach-Object { [int]$_.OwningProcess } | Sort-Object -Unique)
  if ($owners.Count -ne 1) { throw 'app server discovery is absent or ambiguous; set DRAIN_APP_PID or DRAIN_APP_PORT' }
  $owner = $owners[0]
}
$process = Get-Process -Id $owner
$start = $process.StartTime.ToUniversalTime().ToString('o')
while ($true) {
  $process.Refresh()
  if ($process.HasExited) { throw 'app server exited' }
  $io = [DrainIoNative]::Read($process.Handle)
  $tcpListeners = @(Get-NetTCPConnection -OwningProcess $owner -State Listen -ErrorAction SilentlyContinue |
    ForEach-Object { [int]$_.LocalPort } | Sort-Object -Unique)
  $udpListeners = @(Get-NetUDPEndpoint -OwningProcess $owner -ErrorAction SilentlyContinue |
    ForEach-Object { [int]$_.LocalPort } | Sort-Object -Unique)
  $connections = @(Get-NetTCPConnection -OwningProcess $owner -State Established -ErrorAction SilentlyContinue).Count
  $row = [ordered]@{
    at = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    pid = $owner
    start = $start
    cpuMs = $process.TotalProcessorTime.TotalMilliseconds
    ioBytes = [double]($io.ReadTransferCount + $io.WriteTransferCount + $io.OtherTransferCount)
    tcpListeners = $tcpListeners
    udpListeners = $udpListeners
    established = $connections
  }
  [Console]::Out.WriteLine(($row | ConvertTo-Json -Compress))
  [Console]::Out.Flush()
  Start-Sleep -Milliseconds ${SERVER_SAMPLE_MS}
}
`;
}

class ServerTelemetry {
  constructor(target = appTargetFromEnvironment()) {
    this.target = target;
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
      ['-NoLogo', '-NoProfile', '-NonInteractive', '-Command', serverTelemetryScript(this.target)],
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

function cpuMetrics(samples) {
  const busy = samples.map((sample) => sample.busyPercent);
  return {
    ...summarizeNumbers(busy),
    freeMemoryMin: samples.length ? Math.min(...samples.map((sample) => sample.freeMemory)) : null,
    rssMax: samples.length ? Math.max(...samples.map((sample) => sample.rss)) : null,
    samples,
  };
}

function listenerSignature(sample) {
  const normalized = (value) =>
    [...new Set(Array.isArray(value) ? value : value === undefined ? [] : [value])].sort(
      (left, right) => left - right,
    );
  return JSON.stringify({
    pid: sample.pid,
    start: sample.start,
    tcpListeners: normalized(sample.tcpListeners),
    udpListeners: normalized(sample.udpListeners),
  });
}

function serverMetrics(samples, baseline, windowStart, windowEnd) {
  const durationMs = Math.max(1, windowEnd - windowStart);
  if (samples.length < 2) {
    return {
      valid: false,
      reason: 'server-telemetry-insufficient',
      count: samples.length,
      durationMs,
      samples,
    };
  }
  const signature = listenerSignature(baseline);
  const identityValid = samples.every((sample) => listenerSignature(sample) === signature);
  const first = samples[0];
  const last = samples.at(-1);
  const gaps = samples.slice(1).map((sample, index) => sample.at - samples[index].at);
  const startLagMs = first.at - windowStart;
  const endLagMs = windowEnd - last.at;
  const maxGapMs = Math.max(...gaps);
  const coveredMs = last.at - first.at + SERVER_SAMPLE_MS;
  const coverage = Math.min(1, coveredMs / durationMs);
  const cadenceValid =
    durationMs >= SERVER_MIN_WINDOW_MS &&
    startLagMs <= SERVER_EDGE_FRESHNESS_MS &&
    endLagMs <= SERVER_EDGE_FRESHNESS_MS &&
    maxGapMs <= SERVER_MAX_GAP_MS &&
    coverage >= SERVER_MIN_COVERAGE;
  const reason = !identityValid
    ? 'server-listener-changed'
    : !cadenceValid
      ? 'server-telemetry-cadence'
      : null;
  return {
    valid: identityValid && cadenceValid,
    reason,
    identityValid,
    cadenceValid,
    signature,
    durationMs,
    startLagMs,
    endLagMs,
    maxGapMs,
    coveredMs,
    coverage,
    limits: {
      minimumSamples: 2,
      minimumWindowMs: SERVER_MIN_WINDOW_MS,
      edgeFreshnessMs: SERVER_EDGE_FRESHNESS_MS,
      maxGapMs: SERVER_MAX_GAP_MS,
      minimumCoverage: SERVER_MIN_COVERAGE,
    },
    cpuMs: last.cpuMs - first.cpuMs,
    ioBytes: last.ioBytes - first.ioBytes,
    established: summarizeNumbers(samples.map((sample) => sample.established)),
    count: samples.length,
    samples,
  };
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
    normalized.startsWith('diagnostic-output/') ||
    normalized.startsWith('ensaio-resultados/')
  );
}

async function candidateSnapshot() {
  const listed = spawnSync('git', ['ls-files', '-co', '--exclude-standard', '-z'], {
    cwd: root,
    encoding: 'utf8',
    windowsHide: true,
  });
  if (listed.status !== 0) throw new Error('git ls-files failed: ' + listed.stderr);
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
    rows.push(relative.replaceAll('\\', '/') + '\0' + (await sha256File(absolute)));
  }
  const locks = {};
  for (const [relative, expected] of expectedLocks) {
    const actual = await sha256File(path.join(root, relative));
    locks[relative] = actual;
    if (actual !== expected) throw new Error('locked hash drift: ' + relative + '=' + actual);
  }
  const frozen = {};
  for (const [relative, expected] of FROZEN_HASHES) {
    const actual = await sha256File(path.join(root, relative));
    frozen[relative] = actual;
    if (actual !== expected) throw new Error('frozen hash drift: ' + relative + '=' + actual);
  }
  let envHash = null;
  try {
    envHash = await sha256File(path.join(root, '.env'));
  } catch {
    // Missing .env remains an explicit null fingerprint.
  }
  return {
    manifest: sha256(rows.join('\n')),
    fileCount: rows.length,
    exclusions: [
      '.git',
      'node_modules',
      'dist',
      'coverage',
      'diagnostic-output/',
      'ensaio-resultados/',
    ],
    locks,
    frozen,
    envHash,
  };
}

function hashOrder(label, values) {
  return [...values].sort((left, right) => {
    const leftHash = createHash('sha256')
      .update(label + left)
      .digest();
    const rightHash = createHash('sha256')
      .update(label + right)
      .digest();
    const compared = Buffer.compare(leftHash, rightHash);
    return compared || left.localeCompare(right);
  });
}

function buildSchedule() {
  const permutations = ['0LH', '0HL', 'L0H', 'LH0', 'H0L', 'HL0'];
  const candidates = permutations.flatMap((permutation, permutationIndex) =>
    [1, 2].map((replicate) => ({
      id: 'p' + (permutationIndex + 1) + 'r' + replicate,
      permutation,
    })),
  );
  const blocks = hashOrder(
    SCHEDULE_SEED + ':block-order:',
    candidates.map(({ id }) => id),
  ).map((id, index) => {
    const candidate = candidates.find((value) => value.id === id);
    return { ...candidate, block: index + 1 };
  });
  for (const level of ['zero', 'low', 'high']) {
    const positions = blocks.map(({ block }) => String(block));
    const ranked = hashOrder(SCHEDULE_SEED + ':instrument:' + level + ':', positions);
    const probeFirst = new Set(ranked.slice(0, 6).map(Number));
    for (const block of blocks) {
      block[level + 'Order'] = probeFirst.has(block.block) ? 'PL' : 'LP';
    }
  }
  const units = blocks.flatMap((block) =>
    [...block.permutation].map((symbol, position) => {
      const level = SYMBOL_LEVEL[symbol];
      return {
        pilot: false,
        block: block.block,
        blockId: block.id,
        permutation: block.permutation,
        position: position + 1,
        level,
        assignedLevel: level,
        instrumentOrder: block[level + 'Order'],
      };
    }),
  );
  return {
    seed: SCHEDULE_SEED,
    blocks,
    units,
    pilots: ['zero', 'low', 'high'].map((level, index) => ({
      pilot: true,
      pilotIndex: index + 1,
      block: null,
      blockId: null,
      permutation: null,
      position: 1,
      level,
      assignedLevel: level,
      instrumentOrder: index % 2 === 0 ? 'PL' : 'LP',
    })),
  };
}

function transitionMatrix(units) {
  const matrix = {};
  let previous = 'START';
  for (const unit of units) {
    const assignedLevel = unit.assignedLevel ?? unit.level;
    const key = (LEVEL_SYMBOL[previous] ?? previous) + '->' + LEVEL_SYMBOL[assignedLevel];
    matrix[key] = (matrix[key] ?? 0) + 1;
    previous = assignedLevel;
  }
  return matrix;
}

function verifySchedule() {
  const schedule = buildSchedule();
  const blockOrder = schedule.blocks.map((block) => block.permutation);
  if (JSON.stringify(blockOrder) !== JSON.stringify(EXPECTED_BLOCK_ORDER)) {
    throw new Error('block seed mismatch: ' + blockOrder.join(','));
  }
  for (const level of ['zero', 'low', 'high']) {
    const actual = schedule.blocks.map((block) => block[level + 'Order']);
    if (JSON.stringify(actual) !== JSON.stringify(EXPECTED_INSTRUMENT_ORDERS[level])) {
      throw new Error('instrument seed mismatch ' + level + ': ' + actual.join(','));
    }
    const positions = [1, 2, 3].map(
      (position) =>
        schedule.units.filter((unit) => unit.level === level && unit.position === position).length,
    );
    if (JSON.stringify(positions) !== JSON.stringify([4, 4, 4])) {
      throw new Error('position balance mismatch ' + level + ': ' + positions.join(','));
    }
  }
  const permutations = Object.fromEntries(
    EXPECTED_BLOCK_ORDER.map((value) => [
      value,
      blockOrder.filter((item) => item === value).length,
    ]),
  );
  if (Object.values(permutations).some((count) => count !== 2)) {
    throw new Error('permutation multiplicity mismatch');
  }
  const matrix = transitionMatrix(schedule.units);
  const expectedMatrix = {
    'START->L': 1,
    '0->0': 2,
    'H->H': 2,
    'L->L': 1,
    '0->L': 5,
    'L->0': 5,
    '0->H': 5,
    'H->0': 5,
    'L->H': 5,
    'H->L': 5,
  };
  const normalizedMatrix = Object.fromEntries(Object.entries(matrix).sort());
  const normalizedExpected = Object.fromEntries(Object.entries(expectedMatrix).sort());
  if (JSON.stringify(normalizedMatrix) !== JSON.stringify(normalizedExpected)) {
    throw new Error('transition matrix mismatch: ' + JSON.stringify(matrix));
  }
  return { schedule, blockOrder, matrix };
}

async function writeRaw(sink, id, stream, value) {
  const relative = path.join('raw', `${id}.${stream}.log`);
  await sink.write(relative, value);
  return { file: relative.replaceAll('\\', '/'), sha256: sha256(value) };
}

async function runChild({ sink, id, command, args, env, limitMs }) {
  const startedAt = Date.now();
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
  const endedAt = Date.now();
  const stdoutText = Buffer.concat(stdout).toString('utf8');
  const stderrText = Buffer.concat(stderr).toString('utf8');
  const raw = {
    stdout: await writeRaw(sink, id, 'stdout', stdoutText),
    stderr: await writeRaw(sink, id, 'stderr', stderrText),
  };
  return {
    ...result,
    pid,
    startedAt,
    endedAt,
    elapsedMs: endedAt - startedAt,
    limitMs,
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

async function fixedWindow(context, label, milliseconds) {
  const start = Date.now();
  const workersStart = context.regimes.snapshot();
  context.loopDelay.reset();
  await new Promise((resolve) => setTimeout(resolve, milliseconds));
  const end = Date.now();
  const workersEnd = context.regimes.snapshot();
  return {
    label,
    start,
    end,
    durationMs: end - start,
    cpu: cpuMetrics(context.cpu.between(start, end)),
    server: serverMetrics(context.server.between(start, end), context.serverBaseline, start, end),
    eventLoop: eventLoopMetrics(context.loopDelay),
    workers: { start: workersStart, end: workersEnd },
  };
}

async function measuredAction(context, label, action) {
  const start = Date.now();
  const workersStart = context.regimes.snapshot();
  context.loopDelay.reset();
  const value = await action();
  const end = Date.now();
  const workersEnd = context.regimes.snapshot();
  return {
    label,
    start,
    end,
    durationMs: end - start,
    cpu: cpuMetrics(context.cpu.between(start, end)),
    server: serverMetrics(context.server.between(start, end), context.serverBaseline, start, end),
    eventLoop: eventLoopMetrics(context.loopDelay),
    workers: { start: workersStart, end: workersEnd },
    value,
  };
}

function childIntegrity(child) {
  return {
    exited: child.code !== null || child.signal !== null,
    pipesClosed: child.pipesClosed,
    processAliveAfterClose: child.processAliveAfterClose,
    pass:
      (child.code !== null || child.signal !== null) &&
      child.pipesClosed &&
      !child.processAliveAfterClose,
  };
}

function childBudget(instrument, now, unitDeadline) {
  const contractMs = CHILD_CONTRACT_MS[instrument];
  if (!Number.isFinite(contractMs)) throw new Error(`unknown instrument: ${instrument}`);
  const remainingMs = Math.max(0, unitDeadline - now);
  return {
    instrument,
    contractMs,
    remainingMs,
    limitMs: Math.min(contractMs, remainingMs),
    unitLimited: remainingMs <= contractMs,
  };
}

function childTimeoutDisposition(timedOut, budget) {
  if (!timedOut) return 'completed';
  return budget.unitLimited ? 'incomplete-run' : 'instrument-error';
}

function probeLockedAgreement(probe, locked) {
  if (!locked.targetTimeout) return { pass: true, reason: null };
  const compatible = Boolean(probe.censored) || Number(probe.drainMs) >= 9000;
  return {
    pass: compatible,
    reason: compatible ? null : 'probe-locked-divergence',
  };
}

async function executeInstrument(context, descriptor, instrument, unitDeadline) {
  const id =
    (descriptor.pilot ? 'pilot-' + descriptor.pilotIndex : 'block-' + descriptor.block) +
    '-' +
    descriptor.position +
    '-' +
    descriptor.level +
    '-' +
    instrument;
  const budget = childBudget(instrument, Date.now(), unitDeadline);
  if (budget.limitMs <= 0) throw new Error('incomplete-run: unit-wall-time');
  return measuredAction(context, descriptor.level + '-' + instrument, async () => {
    if (instrument === 'probe') {
      const child = await runChild({
        sink: context.sink,
        id,
        command: process.execPath,
        args: [probePath],
        env: { ...process.env },
        limitMs: budget.limitMs,
      });
      const probe = parseProbe(child);
      const integrity = childIntegrity(child);
      const timeoutDisposition = childTimeoutDisposition(child.timedOut, budget);
      if (timeoutDisposition === 'incomplete-run')
        throw new Error('incomplete-run: unit-wall-time');
      if (
        timeoutDisposition === 'instrument-error' ||
        !integrity.pass ||
        child.code !== 0 ||
        !probe.ok
      ) {
        throw new Error(
          'instrument-error: probe-failure ' +
            JSON.stringify({
              timedOut: child.timedOut,
              code: child.code,
              probe,
              integrity,
              budget,
            }),
        );
      }
      return {
        kind: 'probe',
        budget,
        child,
        integrity,
        probe,
        T: probe.censored
          ? { lower: TAU_MS, upper: null, censored: true }
          : { lower: probe.drainMs, upper: probe.drainMs, censored: false },
        yTau: probe.censored ? TAU_MS : Math.min(Number(probe.drainMs), TAU_MS),
      };
    }
    const env = { ...process.env, WEBTRANSPORT_LIVE: '1' };
    delete env.WT_LIVE_NODE_MODULES;
    const child = await runChild({
      sink: context.sink,
      id,
      command: process.execPath,
      args: [
        vitestPath,
        'run',
        lockedPath,
        '--reporter=dot',
        '--maxWorkers=1',
        '--pool=forks',
        '-t',
        LOCKED_TEST_TITLE,
      ],
      env,
      limitMs: budget.limitMs,
    });
    const combined = child.stdoutText + '\n' + child.stderrText;
    const targetTimeout =
      child.code === 1 &&
      child.signal === null &&
      combined.includes('listener baseline after 32 CONNECTs timeout');
    const lockedClass = targetTimeout
      ? 'target-timeout'
      : child.code === 0
        ? 'pass'
        : 'other-failure';
    const integrity = childIntegrity(child);
    const timeoutDisposition = childTimeoutDisposition(child.timedOut, budget);
    if (timeoutDisposition === 'incomplete-run') throw new Error('incomplete-run: unit-wall-time');
    if (
      timeoutDisposition === 'instrument-error' ||
      !integrity.pass ||
      lockedClass === 'other-failure'
    ) {
      throw new Error(
        'instrument-error: locked-other-failure ' +
          JSON.stringify({
            timedOut: child.timedOut,
            code: child.code,
            lockedClass,
            integrity,
            budget,
          }),
      );
    }
    return { kind: 'locked', budget, child, integrity, lockedClass, targetTimeout };
  });
}

function combineCpu(windows) {
  return cpuMetrics(windows.flatMap((window) => window.cpu.samples));
}

function validateWindow(window) {
  if (!window.server.valid) {
    throw new Error(`aborted-integrity: ${window.server.reason ?? 'server-telemetry-invalid'}`);
  }
  if (window.workers.start.failures.length || window.workers.end.failures.length) {
    throw new Error('instrument-error: regime-worker-failure');
  }
}

async function runUnit(context, descriptor) {
  const unitStartedAt = Date.now();
  const unitDeadline = unitStartedAt + UNIT_LIMIT_MS;
  const before = await candidateSnapshot();
  if (before.manifest !== context.reference.manifest) {
    throw new Error('aborted-integrity: candidate drift before unit');
  }
  await context.regimes.quiesce();
  const previousLevel = context.previousLevel;
  const baseline = await fixedWindow(context, descriptor.level + '-baseline', BASELINE_MS);
  validateWindow(baseline);
  const activation = await context.regimes.setLevel(descriptor.assignedLevel);
  const treatmentStart = context.regimes.snapshot(descriptor.assignedLevel);
  const instruments =
    descriptor.instrumentOrder === 'PL' ? ['probe', 'locked'] : ['locked', 'probe'];
  const executions = [];
  for (const instrument of instruments) {
    const washIn = await fixedWindow(
      context,
      descriptor.level + '-' + instrument + '-wash-in',
      WASH_IN_MS,
    );
    validateWindow(washIn);
    const pre = await fixedWindow(
      context,
      descriptor.level + '-' + instrument + '-pre',
      PRE_INSTRUMENT_MS,
    );
    validateWindow(pre);
    const outcome = await executeInstrument(context, descriptor, instrument, unitDeadline);
    validateWindow(outcome);
    executions.push({ instrument, washIn, pre, outcome });
  }
  const treatmentEnd = context.regimes.snapshot(descriptor.assignedLevel);
  const adherence = context.regimes.adherence(
    treatmentStart,
    treatmentEnd,
    descriptor.assignedLevel,
  );
  if (!adherence.pass) throw new Error('instrument-error: treatment-adherence');
  const quiescence = await context.regimes.quiesce();
  const after = await candidateSnapshot();
  if (after.manifest !== context.reference.manifest) {
    throw new Error('aborted-integrity: candidate drift after unit');
  }
  if (Date.now() > unitDeadline) throw new Error('incomplete-run: unit-wall-time');
  const probeExecution = executions.find((entry) => entry.instrument === 'probe');
  const lockedExecution = executions.find((entry) => entry.instrument === 'locked');
  const probe = probeExecution.outcome.value;
  const locked = lockedExecution.outcome.value;
  const agreement = probeLockedAgreement(probe.probe, locked);
  if (!agreement.pass) throw new Error(`instrument-error: ${agreement.reason}`);
  const combinedPostAckCpu = combineCpu(executions.map((entry) => entry.pre));
  const firstStage = {
    baselineCpuP50: baseline.cpu.p50,
    postAckCombinedCpuP50: combinedPostAckCpu.p50,
    F: combinedPostAckCpu.p50 - baseline.cpu.p50,
    combinedPostAckCpu,
  };
  const endedAt = Date.now();
  const record = {
    type: descriptor.pilot ? 'pilot' : 'unit',
    runId: context.runId,
    ...descriptor,
    previousLevel,
    startedAt: unitStartedAt,
    endedAt,
    durationMs: endedAt - unitStartedAt,
    P: context.configuration.P,
    L: context.configuration.L,
    H: context.configuration.H,
    before,
    after,
    baseline,
    activation,
    washIn: executions.map((entry) => ({ instrument: entry.instrument, window: entry.washIn })),
    executions,
    probe: {
      T: probe.T,
      yTau: probe.yTau,
      censored: probe.probe.censored,
      drainMs: probe.probe.drainMs ?? null,
      inventory: probe.probe,
    },
    locked: { class: locked.lockedClass, targetTimeout: locked.targetTimeout },
    firstStage,
    adherence,
    quiescence,
    probeLockedAgreement: agreement,
    cleanupObserved: executions.map((entry) => ({
      instrument: entry.instrument,
      child: entry.outcome.value.integrity,
      probeInventory:
        entry.instrument === 'probe'
          ? {
              baseline: entry.outcome.value.probe.baseline,
              finalInventory: entry.outcome.value.probe.finalInventory,
              listenerPort: entry.outcome.value.probe.listenerPort,
            }
          : null,
      lockedClass: entry.instrument === 'locked' ? entry.outcome.value.lockedClass : null,
    })),
  };
  await context.sink.append(record);
  context.previousLevel = descriptor.level;
  return record;
}

function nearestRank(values, fraction = 0.5) {
  if (!values.length) return null;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.max(1, Math.ceil(fraction * sorted.length)) - 1];
}

function mean(values) {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function byLevel(units, level) {
  return units.filter((unit) => (unit.assignedLevel ?? unit.level) === level);
}

function firstStageEvidence(units) {
  const medians = Object.fromEntries(
    ['zero', 'low', 'high'].map((level) => [
      level,
      nearestRank(byLevel(units, level).map((unit) => unit.firstStage.F)),
    ]),
  );
  const checks = {
    highMinusZero: medians.high - medians.zero >= 10,
    lowMinusZero: medians.low - medians.zero >= 4,
    highMinusLow: medians.high - medians.low >= 4,
    monotonic: medians.zero <= medians.low && medians.low <= medians.high,
    adherence: units.every((unit) => unit.adherence.pass),
  };
  return { medians, checks, strong: Object.values(checks).every(Boolean) };
}

function blockDeltas(units) {
  return Array.from({ length: BLOCK_COUNT }, (_, index) => {
    const block = index + 1;
    const values = Object.fromEntries(
      ['zero', 'low', 'high'].map((level) => [
        level,
        units.find((unit) => unit.block === block && unit.level === level).probe.yTau,
      ]),
    );
    return {
      block,
      values,
      highZero: values.high - values.zero,
      lowZero: values.low - values.zero,
      highLow: values.high - values.low,
    };
  });
}

function probeEffectEvidence(units) {
  const deltas = blockDeltas(units);
  const values = Object.fromEntries(
    ['zero', 'low', 'high'].map((level) => [
      level,
      byLevel(units, level).map((unit) => unit.probe.yTau),
    ]),
  );
  const censors = Object.fromEntries(
    ['zero', 'low', 'high'].map((level) => [
      level,
      byLevel(units, level).filter((unit) => unit.probe.censored).length,
    ]),
  );
  const positive = deltas.filter((delta) => delta.highZero > 0);
  const firstHalf = deltas.slice(0, 6).filter((delta) => delta.highZero > 0).length;
  const secondHalf = deltas.slice(6).filter((delta) => delta.highZero > 0).length;
  const means = Object.fromEntries(
    ['zero', 'low', 'high'].map((level) => [level, mean(values[level])]),
  );
  const checks = {
    positive10of12: positive.length >= 10,
    splitFirst5of6: firstHalf >= 5,
    splitSecond5of6: secondHalf >= 5,
    medianDelta: nearestRank(deltas.map((delta) => delta.highZero)) >= 1500,
    meanHighZero: means.high - means.zero >= 1500,
    meanLowZero: means.low - means.zero >= 500,
    meanHighLow: means.high - means.low >= 500,
    monotonic: means.zero <= means.low && means.low <= means.high,
    extraHighCensors: censors.high - censors.zero >= 2,
  };
  return {
    deltas,
    values,
    censors,
    means,
    positiveCount: positive.length,
    firstHalf,
    secondHalf,
    checks,
    effect: Object.values(checks).every(Boolean),
  };
}

function lockedEffectEvidence(units) {
  const count = (level, start, end) =>
    units.filter(
      (unit) =>
        (unit.assignedLevel ?? unit.level) === level &&
        unit.block >= start &&
        unit.block <= end &&
        unit.locked.targetTimeout,
    ).length;
  const totals = Object.fromEntries(
    ['zero', 'low', 'high'].map((level) => [level, count(level, 1, BLOCK_COUNT)]),
  );
  const halves = {
    first: { zero: count('zero', 1, 6), high: count('high', 1, 6) },
    second: { zero: count('zero', 7, 12), high: count('high', 7, 12) },
  };
  const checks = {
    highMinusZero: totals.high - totals.zero >= 2,
    firstHalf: halves.first.high - halves.first.zero >= 1,
    secondHalf: halves.second.high - halves.second.zero >= 1,
  };
  return { totals, halves, checks, effect: Object.values(checks).every(Boolean) };
}

function carryoverEvidence(units) {
  const eligible = units.filter((unit) => unit.previousLevel);
  const groups = Object.fromEntries(
    ['zero', 'low', 'high'].map((level) => [
      level,
      eligible.filter((unit) => unit.previousLevel === level),
    ]),
  );
  const medians = {};
  for (const level of ['zero', 'low', 'high']) {
    medians[level] = {
      cpu: nearestRank(groups[level].map((unit) => unit.baseline.cpu.p50)),
      eventLoopP95: nearestRank(groups[level].map((unit) => unit.baseline.eventLoop.p95Ms)),
      rss: nearestRank(groups[level].map((unit) => unit.baseline.cpu.rssMax)),
    };
  }
  const differences = {
    highZeroCpu: medians.high.cpu - medians.zero.cpu,
    lowZeroCpu: medians.low.cpu - medians.zero.cpu,
    highZeroEventLoop: medians.high.eventLoopP95 - medians.zero.eventLoopP95,
    lowZeroEventLoop: medians.low.eventLoopP95 - medians.zero.eventLoopP95,
    highZeroRss: medians.high.rss - medians.zero.rss,
    lowZeroRss: medians.low.rss - medians.zero.rss,
  };
  const triggers = {
    highZeroCpu: differences.highZeroCpu >= 10,
    lowZeroCpu: differences.lowZeroCpu >= 6,
    highZeroEventLoop: differences.highZeroEventLoop >= 20,
    lowZeroEventLoop: differences.lowZeroEventLoop >= 20,
    highZeroRss: differences.highZeroRss >= 64 * 1024 * 1024,
    lowZeroRss: differences.lowZeroRss >= 64 * 1024 * 1024,
  };
  return {
    groups: Object.fromEntries(
      Object.entries(groups).map(([level, values]) => [level, values.length]),
    ),
    medians,
    differences,
    triggers,
    material: Object.values(triggers).some(Boolean),
  };
}

function slowEvidence(units, level) {
  const values = byLevel(units, level).map((unit) => unit.probe.yTau);
  const tailCount = values.filter((value) => value >= 9000).length;
  const median = nearestRank(values);
  return { values, tailCount, median, slow: tailCount >= 7 && median >= 9000 };
}

function classifyDataset(units, { complete = true, integrity = true } = {}) {
  if (!integrity || !complete || units.length !== BLOCK_COUNT * 3) {
    return { outcome: 'incomplete-run', classification: null, substantive: false };
  }
  const firstStage = firstStageEvidence(units);
  const probeEffect = probeEffectEvidence(units);
  const lockedEffect = lockedEffectEvidence(units);
  const carryover = carryoverEvidence(units);
  const slow = Object.fromEntries(
    ['zero', 'low', 'high'].map((level) => [level, slowEvidence(units, level)]),
  );
  let classification;
  let reasons = [];
  if (carryover.material) classification = 'inconclusive-carryover';
  else if (!firstStage.strong) classification = 'inconclusive-treatment-not-separated';
  else if (probeEffect.effect && lockedEffect.effect)
    classification = 'assigned-regime-gate-effect';
  else if (Object.values(slow).every((value) => value.slow)) classification = 'product-suspect';
  else {
    classification = 'inconclusive-no-contrast';
    reasons = [
      probeEffect.effect && !lockedEffect.effect
        ? 'probe-only-effect'
        : !probeEffect.effect && lockedEffect.effect
          ? 'locked-only-effect'
          : 'effect-below-engineering-threshold',
    ];
  }
  return {
    outcome: classification,
    classification,
    substantive: true,
    reasons,
    firstStage,
    probeEffect,
    lockedEffect,
    carryover,
    slow,
    transitionMatrix: transitionMatrix(units),
  };
}

function operatingCharacteristics(probability) {
  let atLeastFive = 0;
  for (let wins = 5; wins <= 6; wins++) {
    const combinations = wins === 5 ? 6 : 1;
    atLeastFive += combinations * probability ** wins * (1 - probability) ** (6 - wins);
  }
  return atLeastFive ** 2;
}

async function writeArtifactIndex(context, summary) {
  const rawFiles = await readdir(path.join(context.output, 'raw'));
  const title = 'Sprint 02E - randomized WebTransport drain diagnosis';
  const classification = summary.analysis.classification ?? 'none (incomplete run)';
  const lines = [
    '---',
    'kind: spec',
    'title: "' + title + '"',
    '---',
    '',
    '# ' + title,
    '',
    'Mechanical classification: **' + classification + '**.',
    '',
    '- Started: ' + new Date(context.startedAt).toISOString(),
    '- Ended: ' + new Date().toISOString(),
    '- Official blocks: ' + summary.completedBlocks + '/12',
    '- Official observations: ' + summary.units.length + '/36',
    '- Candidate manifest: ' + context.reference.manifest,
    '- Locks: 8/8; frozen instruments: 3/3',
    '',
    'Executable evidence:',
    '',
    '- [raw.jsonl](raw.jsonl)',
    '- [summary.json](summary.json)',
    '- [run-state.json](run-state.json)',
    ...(rawFiles.length ? ['- [raw/](raw/)'] : []),
    '',
    'The probe is an unlocked timer. Locked tails are analyzed separately and are required for any gate-effect class.',
    '',
  ];
  await context.sink.write('index.md', lines.join('\n'));
}

async function runWorker(output, startedAt, runId) {
  const sink = await SinkGuard.attach(output, runId);
  const scheduleAudit = verifySchedule();
  const P = typeof availableParallelism === 'function' ? availableParallelism() : cpus().length;
  const H = Math.min(8, Math.max(4, Math.ceil(P / 3)));
  const L = Math.max(1, Math.floor(H / 2));
  const context = {
    output,
    sink,
    runId,
    startedAt,
    measureStopAt: startedAt + MEASURE_STOP_MS,
    cpu: new CpuSampler(),
    regimes: new RegimeWorkers({ high: H, low: L }),
    server: new ServerTelemetry(),
    loopDelay: monitorEventLoopDelay({ resolution: 20 }),
    reference: null,
    serverBaseline: null,
    previousLevel: null,
    configuration: { P, L, H },
  };
  const pilots = [];
  const units = [];
  let terminalWritten = false;
  const terminal = async (outcome, extra = {}) => {
    if (terminalWritten) return;
    const value = {
      status: 'terminal',
      outcome,
      at: new Date().toISOString(),
      manifest: context.reference?.manifest ?? null,
      ...extra,
    };
    await sink.append({ type: 'terminal', ...value });
    await sink.writeJson('run-state.json', value);
    terminalWritten = true;
  };

  try {
    context.reference = await candidateSnapshot();
    await sink.writeJson('run-state.json', {
      status: 'running',
      startedAt,
      deadline: startedAt + WALL_MS,
      stage: 'worker-start',
      runId,
    });
    await sink.append({
      type: 'start',
      runId,
      startedAt,
      configuration: context.configuration,
      serverTarget: context.server.target,
      schedule: scheduleAudit,
      reference: context.reference,
    });
    context.cpu.start();
    context.loopDelay.enable();
    context.serverBaseline = await context.server.start();
    await sink.append({
      type: 'server-baseline',
      target: context.server.target,
      baseline: context.serverBaseline,
    });
    await context.regimes.start();

    const beforePilots = await candidateSnapshot();
    if (beforePilots.manifest !== context.reference.manifest) {
      throw new Error('aborted-integrity: candidate drift before pilots');
    }
    for (const descriptor of scheduleAudit.schedule.pilots) {
      pilots.push(await runUnit(context, descriptor));
    }
    context.previousLevel = null;
    for (const block of scheduleAudit.schedule.blocks) {
      if (Date.now() >= context.measureStopAt) {
        const analysis = classifyDataset(units, { complete: false, integrity: true });
        const summary = {
          analysis,
          startedAt,
          endedAt: Date.now(),
          reference: context.reference,
          configuration: context.configuration,
          serverTarget: context.server.target,
          serverBaseline: context.serverBaseline,
          schedule: scheduleAudit,
          pilots,
          units,
          completedBlocks: new Set(units.map((unit) => unit.block)).size,
        };
        await sink.writeJson('summary.json', summary);
        await writeArtifactIndex(context, summary);
        await terminal('incomplete-run', {
          classification: null,
          substantive: false,
          units: units.length,
          completedBlocks: summary.completedBlocks,
          manifest: context.reference.manifest,
        });
        return;
      }
      for (const descriptor of scheduleAudit.schedule.units.filter(
        (unit) => unit.block === block.block,
      )) {
        units.push(await runUnit(context, descriptor));
      }
    }

    const analysis = classifyDataset(units);
    const summary = {
      analysis,
      startedAt,
      endedAt: Date.now(),
      reference: context.reference,
      configuration: context.configuration,
      serverTarget: context.server.target,
      serverBaseline: context.serverBaseline,
      schedule: scheduleAudit,
      pilots,
      units,
      completedBlocks: BLOCK_COUNT,
      operatingCharacteristics: Object.fromEntries(
        [0.5, 0.6, 0.7, 0.75, 0.8, 0.9].map((probability) => [
          probability,
          operatingCharacteristics(probability),
        ]),
      ),
    };
    await sink.writeJson('summary.json', summary);
    await writeArtifactIndex(context, summary);
    await terminal(analysis.outcome, {
      classification: analysis.classification,
      substantive: analysis.substantive,
      units: units.length,
      completedBlocks: BLOCK_COUNT,
      manifest: context.reference.manifest,
    });
  } catch (error) {
    const message = String(error?.message ?? error);
    const nonSubstantiveOutcome = message.startsWith('incomplete-run')
      ? 'incomplete-run'
      : message.startsWith('aborted-integrity')
        ? 'aborted-integrity'
        : null;
    if (nonSubstantiveOutcome) {
      const analysis = {
        outcome: nonSubstantiveOutcome,
        classification: null,
        substantive: false,
        error: error?.stack ?? error?.message ?? String(error),
      };
      const summary = {
        analysis,
        startedAt,
        endedAt: Date.now(),
        reference: context.reference,
        configuration: context.configuration,
        serverTarget: context.server.target,
        serverBaseline: context.serverBaseline,
        schedule: scheduleAudit,
        pilots,
        units,
        completedBlocks: new Set(units.map((unit) => unit.block)).size,
      };
      await sink.writeJson('summary.json', summary);
      await writeArtifactIndex(context, summary);
      await terminal(nonSubstantiveOutcome, {
        classification: null,
        substantive: false,
        units: units.length,
        completedBlocks: summary.completedBlocks,
        manifest: context.reference?.manifest ?? null,
        error: analysis.error,
      });
      return;
    }
    try {
      await terminal('instrument-error', {
        classification: null,
        substantive: false,
        error: error?.stack ?? error?.message ?? String(error),
      });
    } catch {
      // The independent supervisor terminal remains authoritative if the sink was lost.
    }
    throw error;
  } finally {
    await context.regimes.stop();
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
    summary.analysis?.outcome !== state.outcome
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
  const stageParent = await mkdtemp(path.join(tmpdir(), 'discord-locutor-02e-stage-'));
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
  const stageParent = await mkdtemp(path.join(tmpdir(), 'discord-locutor-02e-failure-stage-'));
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
        'title: "Sprint 02E - closed diagnostic instrument failure"',
        '---',
        '',
        '# Closed diagnostic instrument failure',
        '',
        'The execution lost or could not verify its sink. No randomized classification was published.',
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
  const spool = await mkdtemp(path.join(tmpdir(), `discord-locutor-02e-${startedAt}-`));
  const terminalDirectory = await mkdtemp(
    path.join(tmpdir(), `discord-locutor-02e-terminal-${startedAt}-`),
  );
  const terminalFile = path.join(terminalDirectory, 'supervisor-terminal.json');
  await SinkGuard.create(spool, runId);
  process.stdout.write(
    `DRAIN_RANDOMIZED_SPOOL ${JSON.stringify({ runId, spool, terminalFile })}\n`,
  );

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
    process.stdout.write(`DRAIN_RANDOMIZED_TERMINAL ${JSON.stringify(failedTerminal)}\n`);
    process.exitCode = 1;
    return;
  }

  process.stdout.write(`DRAIN_RANDOMIZED_TERMINAL ${JSON.stringify(supervisorTerminal)}\n`);
  if (supervisorTerminal.outcome === 'instrument-error') process.exitCode = 1;
}

async function createHealthyCanarySink(reference) {
  const runId = randomUUID();
  const spool = await mkdtemp(path.join(tmpdir(), 'discord-locutor-02e-healthy-canary-'));
  const sink = await SinkGuard.create(spool, runId);
  const analysis = {
    outcome: 'inconclusive-no-contrast',
    classification: 'inconclusive-no-contrast',
    substantive: true,
  };
  await sink.append({ type: 'start', reference });
  await sink.writeJson('summary.json', { analysis, reference });
  await sink.write(
    'index.md',
    ['---', 'kind: spec', 'title: "healthy canary"', '---', '', '# Healthy canary', ''].join('\n'),
  );
  const terminal = {
    status: 'terminal',
    outcome: analysis.outcome,
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

function syntheticUnits(profile = 'gate') {
  const schedule = buildSchedule();
  let previousLevel = null;
  return schedule.units.map((descriptor) => {
    let yTau = descriptor.level === 'zero' ? 1000 : descriptor.level === 'low' ? 2000 : 4000;
    let censored = false;
    let targetTimeout = false;
    let firstStageF = descriptor.level === 'zero' ? 0 : descriptor.level === 'low' ? 5 : 15;
    if (profile === 'gate' || profile === 'probe-only' || profile === 'carryover') {
      if (descriptor.level === 'high' && [1, 7].includes(descriptor.block)) {
        yTau = 10_000;
        censored = true;
        targetTimeout = profile !== 'probe-only';
      }
    } else if (profile === 'product') {
      yTau = 9500;
    } else if (profile === 'weak') {
      yTau = 1000;
      firstStageF = 0;
    } else if (profile === 'locked-only') {
      yTau = 1000;
      targetTimeout = descriptor.level === 'high' && [1, 7].includes(descriptor.block);
    } else {
      yTau = 1000;
    }
    const baselineCpu =
      profile === 'carryover' && previousLevel === 'high'
        ? 35
        : profile === 'carryover' && previousLevel === 'low'
          ? 23
          : 10;
    const unit = {
      ...descriptor,
      previousLevel,
      observedLevel: descriptor.level,
      probe: {
        yTau,
        censored,
        drainMs: censored ? null : yTau,
        T: censored
          ? { lower: TAU_MS, upper: null, censored: true }
          : { lower: yTau, upper: yTau, censored: false },
      },
      locked: {
        targetTimeout,
        class: targetTimeout ? 'target-timeout' : 'pass',
      },
      firstStage: { F: firstStageF },
      adherence: { pass: true },
      baseline: {
        cpu: { p50: baselineCpu, rssMax: 100 * 1024 * 1024 },
        eventLoop: { p95Ms: 2 },
      },
    };
    previousLevel = descriptor.level;
    return unit;
  });
}

function assertClassification(profile, expected, reason = null) {
  const result = classifyDataset(syntheticUnits(profile));
  if (result.classification !== expected) {
    throw new Error(
      'classification canary failed ' +
        profile +
        ': expected=' +
        expected +
        ' actual=' +
        result.classification,
    );
  }
  if (reason && !result.reasons.includes(reason)) {
    throw new Error('classification reason canary failed ' + profile + ': ' + result.reasons);
  }
  return result;
}

function classificationCanaries() {
  const gate = assertClassification('gate', 'assigned-regime-gate-effect');
  const probeOnly = assertClassification(
    'probe-only',
    'inconclusive-no-contrast',
    'probe-only-effect',
  );
  const lockedOnly = assertClassification(
    'locked-only',
    'inconclusive-no-contrast',
    'locked-only-effect',
  );
  const product = assertClassification('product', 'product-suspect');
  const weak = assertClassification('weak', 'inconclusive-treatment-not-separated');
  const carryover = assertClassification('carryover', 'inconclusive-carryover');
  const incomplete = classifyDataset(syntheticUnits('gate').slice(0, 35), { complete: false });
  if (incomplete.classification !== null || incomplete.substantive) {
    throw new Error('incomplete-run classification canary failed');
  }

  const six = syntheticUnits('none');
  let changed = 0;
  for (const unit of six) {
    if (unit.level === 'zero' && changed < 6) {
      unit.probe.yTau = 9500;
      changed += 1;
    }
  }
  const exactlySixSlow = slowEvidence(six, 'zero');
  if (exactlySixSlow.slow) throw new Error('exactly-six slow canary failed');
  const seven = syntheticUnits('none');
  changed = 0;
  for (const unit of seven) {
    if (unit.level === 'zero' && changed < 7) {
      unit.probe.yTau = 9500;
      changed += 1;
    }
  }
  const exactlySevenSlow = slowEvidence(seven, 'zero');
  if (!exactlySevenSlow.slow) throw new Error('exactly-seven slow canary failed');

  const itt = syntheticUnits('gate');
  const before = classifyDataset(itt).classification;
  for (const unit of itt) unit.observedLevel = unit.level === 'high' ? 'zero' : 'high';
  const after = classifyDataset(itt).classification;
  if (before !== after) throw new Error('ITT immutable-label canary failed');

  const censor = syntheticUnits('gate').find((unit) => unit.level === 'high' && unit.block === 1);
  if (
    !censor.probe.censored ||
    censor.probe.T.lower !== TAU_MS ||
    censor.probe.T.upper !== null ||
    censor.probe.yTau !== TAU_MS
  ) {
    throw new Error('censor/Ytau canary failed');
  }

  const expectedOperating = new Map([
    [0.5, 0.011962890625],
    [0.6, 0.0544195584],
    [0.7, 0.17654703062499993],
    [0.75, 0.2850871682167053],
    [0.8, 0.4294967296],
    [0.9, 0.7845264902250001],
  ]);
  const operating = {};
  for (const [probability, expected] of expectedOperating) {
    const actual = operatingCharacteristics(probability);
    if (Math.abs(actual - expected) > 1e-10) {
      throw new Error('operating characteristic mismatch p=' + probability + ' actual=' + actual);
    }
    operating[probability] = actual;
  }

  return {
    gate: gate.classification,
    probeOnly: { classification: probeOnly.classification, reasons: probeOnly.reasons },
    lockedOnly: { classification: lockedOnly.classification, reasons: lockedOnly.reasons },
    product: product.classification,
    weak: weak.classification,
    carryover: carryover.classification,
    incomplete,
    exactlySixSlow,
    exactlySevenSlow,
    ittImmutable: before === after,
    censor: censor.probe,
    operating,
  };
}

function childBudgetCanaries() {
  const slowValidLocked = childBudget('locked', 10_000, 80_000);
  const slowValidElapsedMs = 60_000;
  if (
    slowValidLocked.limitMs !== 65_000 ||
    slowValidElapsedMs > slowValidLocked.limitMs ||
    slowValidLocked.unitLimited
  ) {
    throw new Error('slow-valid locked child budget canary failed');
  }
  if (childTimeoutDisposition(false, slowValidLocked) !== 'completed') {
    throw new Error('slow-valid locked child disposition canary failed');
  }

  const unitTimeout = childBudget('locked', 65_000, 80_000);
  if (
    unitTimeout.limitMs !== 15_000 ||
    !unitTimeout.unitLimited ||
    childTimeoutDisposition(true, unitTimeout) !== 'incomplete-run'
  ) {
    throw new Error('unit timeout budget canary failed');
  }

  const instrumentTimeout = childBudget('probe', 0, 80_000);
  if (
    instrumentTimeout.limitMs !== 50_000 ||
    instrumentTimeout.unitLimited ||
    childTimeoutDisposition(true, instrumentTimeout) !== 'instrument-error'
  ) {
    throw new Error('instrument timeout budget canary failed');
  }

  return {
    slowValidLocked: { ...slowValidLocked, slowValidElapsedMs },
    unitTimeout,
    instrumentTimeout,
  };
}

function probeLockedAgreementCanaries() {
  const negative = probeLockedAgreement(
    { censored: false, drainMs: 8999 },
    { targetTimeout: true },
  );
  const positiveThreshold = probeLockedAgreement(
    { censored: false, drainMs: 9000 },
    { targetTimeout: true },
  );
  const positiveCensored = probeLockedAgreement(
    { censored: true, drainMs: null },
    { targetTimeout: true },
  );
  const lockedPass = probeLockedAgreement(
    { censored: false, drainMs: 100 },
    { targetTimeout: false },
  );
  if (negative.pass || negative.reason !== 'probe-locked-divergence') {
    throw new Error('probe/locked divergence negative canary failed');
  }
  if (!positiveThreshold.pass || !positiveCensored.pass || !lockedPass.pass) {
    throw new Error('probe/locked divergence positive canary failed');
  }
  return { negative, positiveThreshold, positiveCensored, lockedPass };
}

function serverTelemetryCanaries() {
  const baseline = {
    at: 0,
    pid: 42,
    start: '2026-08-27T00:00:00.000Z',
    cpuMs: 0,
    ioBytes: 0,
    tcpListeners: [3210],
    udpListeners: [4567],
    established: 0,
  };
  const sample = (at, overrides = {}) => ({
    ...baseline,
    at,
    cpuMs: at / 10,
    ioBytes: at * 2,
    ...overrides,
  });
  const healthy = serverMetrics(
    [0, 1000, 2000, 3000, 4000, 5000].map((at) => sample(at)),
    baseline,
    0,
    5000,
  );
  const sparse = serverMetrics([sample(0)], baseline, 0, 5000);
  const tooShort = serverMetrics([sample(0), sample(1000)], baseline, 0, 1000);
  const starved = serverMetrics([sample(0), sample(1000), sample(5000)], baseline, 0, 5000);
  const changed = serverMetrics(
    [sample(0), sample(1000, { pid: 99 }), sample(2000), sample(3000), sample(4000)],
    baseline,
    0,
    4000,
  );
  if (!healthy.valid || !healthy.cadenceValid || !healthy.identityValid) {
    throw new Error('healthy server telemetry canary failed');
  }
  if (sparse.valid || sparse.reason !== 'server-telemetry-insufficient') {
    throw new Error('sparse server telemetry canary failed');
  }
  if (tooShort.valid || tooShort.reason !== 'server-telemetry-cadence') {
    throw new Error('short server telemetry canary failed');
  }
  if (starved.valid || starved.reason !== 'server-telemetry-cadence') {
    throw new Error('starved server telemetry canary failed');
  }
  if (changed.valid || changed.reason !== 'server-listener-changed') {
    throw new Error('server identity telemetry canary failed');
  }

  const targets = {
    pid: appTargetFromEnvironment({ DRAIN_APP_PID: '123' }),
    explicitPort: appTargetFromEnvironment({ DRAIN_APP_PORT: '3210' }),
    inheritedPort: appTargetFromEnvironment({ PORT: '4321' }),
    discovered: appTargetFromEnvironment({}),
  };
  if (
    targets.pid.kind !== 'pid' ||
    targets.pid.value !== 123 ||
    targets.explicitPort.kind !== 'port' ||
    targets.explicitPort.value !== 3210 ||
    targets.inheritedPort.kind !== 'port' ||
    targets.inheritedPort.value !== 4321 ||
    targets.discovered.kind !== 'discover-node-server'
  ) {
    throw new Error('server target selection canary failed');
  }
  return { healthy, sparse, tooShort, starved, changed, targets };
}

async function workerRegimeCanary() {
  const P = typeof availableParallelism === 'function' ? availableParallelism() : cpus().length;
  const H = Math.min(8, Math.max(4, Math.ceil(P / 3)));
  const L = Math.max(1, Math.floor(H / 2));
  const regimes = new RegimeWorkers({ high: H, low: L });
  const evidence = { P, L, H };
  try {
    await regimes.start();
    const zeroStart = regimes.snapshot('zero');
    await new Promise((resolve) => setTimeout(resolve, 300));
    const zeroEnd = regimes.snapshot('zero');
    evidence.zero = regimes.adherence(zeroStart, zeroEnd, 'zero');
    await regimes.setLevel('low');
    const lowStart = regimes.snapshot('low');
    await new Promise((resolve) => setTimeout(resolve, 300));
    const lowEnd = regimes.snapshot('low');
    evidence.low = regimes.adherence(lowStart, lowEnd, 'low');
    await regimes.setLevel('high');
    const highStart = regimes.snapshot('high');
    await new Promise((resolve) => setTimeout(resolve, 300));
    const highEnd = regimes.snapshot('high');
    evidence.high = regimes.adherence(highStart, highEnd, 'high');
    evidence.quiescence = await regimes.quiesce();
    if (!evidence.zero.pass || !evidence.low.pass || !evidence.high.pass) {
      throw new Error('worker regime adherence canary failed');
    }
    await regimes.workers[0].worker.terminate();
    await new Promise((resolve) => setTimeout(resolve, 50));
    let failedClosed = false;
    try {
      regimes.assertHealthy();
    } catch {
      failedClosed = true;
    }
    if (!failedClosed) throw new Error('worker crash did not fail closed');
    evidence.crashFailClosed = failedClosed;
  } finally {
    await regimes.stop();
  }
  evidence.zeroResidualThreads = regimes.workers.every(
    (entry) => entry.worker.threadId === -1 || entry.exitCode !== null,
  );
  if (!evidence.zeroResidualThreads) throw new Error('worker thread residual canary failed');
  return evidence;
}

async function runSelfCheck() {
  const schedule = verifySchedule();
  const classifications = classificationCanaries();
  const childBudgets = childBudgetCanaries();
  const probeLocked = probeLockedAgreementCanaries();
  const serverTelemetry = serverTelemetryCanaries();
  const reference = await candidateSnapshot();
  const workers = await workerRegimeCanary();

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
        JSON.stringify({ schemaVersion: 1, runId, replaced: true }) + '\n',
      );
    },
  );
  const jsonlTruncationFailClosed = await mutationCanary(
    reference,
    'jsonl-truncation',
    async (spool) => writeFile(path.join(spool, 'raw.jsonl'), ''),
  );

  const failureParent = await mkdtemp(path.join(tmpdir(), 'discord-locutor-02e-failure-parent-'));
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
  const healthyParent = await mkdtemp(path.join(tmpdir(), 'discord-locutor-02e-healthy-parent-'));
  const healthyOutput = path.join(healthyParent, 'published');
  const healthyTerminal = {
    schemaVersion: 1,
    status: 'terminal',
    outcome: 'inconclusive-no-contrast',
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

  if (!sinkLossFailClosed) throw new Error('sink loss did not fail closed');
  if (!sentinelReplacementFailClosed) throw new Error('sentinel replacement did not fail closed');
  if (!jsonlTruncationFailClosed) throw new Error('JSONL truncation did not fail closed');
  if (!classificationAbsent) throw new Error('sink loss published a classification');
  if (!redundantTerminalReadable) throw new Error('redundant terminal was not readable');
  if (!atomicPublishVerified) throw new Error('atomic publication canary failed');

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
  process.stdout.write(
    JSON.stringify({
      ok: true,
      schedule: {
        blocks: schedule.blockOrder,
        transitions: schedule.matrix,
        observations: schedule.schedule.units.length,
      },
      classifications,
      childBudgets,
      probeLocked,
      serverTelemetry,
      workers,
      manifest: reference.manifest,
      locks: 8,
      frozen: 3,
      sinkLossFailClosed,
      sentinelReplacementFailClosed,
      jsonlTruncationFailClosed,
      classificationAbsent,
      redundantTerminalReadable,
      atomicPublishVerified,
      zeroResidual: true,
    }) + '\n',
  );
}

if (!isMainThread && workerData?.mode === 'regime-worker') {
  runRegimeWorker();
} else {
  const options = parseArgs();
  if (options.mode === 'self-check') await runSelfCheck();
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
}
