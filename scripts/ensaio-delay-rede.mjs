#!/usr/bin/env node

/**
 * Ensaio numerico e reversivel de atraso sob rede degradada.
 *
 * O processo controla somente um perfil dedicado do Chrome via CDP. O backend
 * padrao emula as condicoes no navegador; --impairment wsl-netem aplica tc
 * netem no proxy/TURN WSL atravessado pelos pacotes reais. O caminho medido e o
 * backend ficam etiquetados em toda amostra.
 */

import { execFile, spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import process from 'node:process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { createInterface } from 'node:readline/promises';
import {
  CRITERIOS,
  evaluateCalibration,
  evaluateCaptureSelection,
  evaluateDelaySeries,
  isExpectedPageReady,
  toCsv,
} from './ensaio-delay-lib.mjs';

const execFileAsync = promisify(execFile);
const ROOT = fileURLToPath(new URL('..', import.meta.url));
const DEFAULT_ORIGIN = 'http://localhost:3100';
const DEFAULT_CDP_PORT = 9333;
const SAMPLE_EVERY_MS = 250;
const PROCESS_EVERY_MS = 1000;
const CLOCK_DELAYS = [0, 500, 1000, 1500];
const IMPAIRMENT_DELAYS = [0, 400, 800, 1200];
const WEBTRANSPORT_IMPAIRMENT_DELAYS = [0, 100, 200, 300];
let impairmentBackend = 'cdp';
let netemHandlesP2p = false;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function runStreaming(file, args, { cwd, env = process.env, timeoutMs = 120_000 }) {
  return new Promise((resolve, reject) => {
    const child = spawn(file, args, {
      cwd,
      env,
      windowsHide: false,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let timedOut = false;
    child.stdout.on('data', (chunk) => process.stdout.write(chunk));
    child.stderr.on('data', (chunk) => process.stderr.write(chunk));
    const timer = setTimeout(() => {
      timedOut = true;
      if (process.platform === 'win32') {
        spawn('taskkill.exe', ['/pid', String(child.pid), '/t', '/f'], {
          windowsHide: true,
          stdio: 'ignore',
        });
      } else {
        child.kill('SIGKILL');
      }
    }, timeoutMs);
    child.once('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once('close', (code, signal) => {
      clearTimeout(timer);
      if (timedOut) {
        reject(new Error(`Automacao excedeu ${timeoutMs} ms e foi encerrada com a arvore filha.`));
      } else if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Automacao terminou com code=${code} signal=${signal ?? 'none'}.`));
      }
    });
  });
}

function parseArgs(argv) {
  const args = { command: 'ajuda' };
  const rest = [...argv];
  if (rest[0] && !rest[0].startsWith('--')) args.command = rest.shift();
  while (rest.length) {
    const token = rest.shift();
    if (!token.startsWith('--')) throw new Error(`Argumento inesperado: ${token}`);
    const key = token.slice(2).replaceAll('-', '_');
    if (!rest[0] || rest[0].startsWith('--')) args[key] = true;
    else args[key] = rest.shift();
  }
  return args;
}

function normalizeTransport(value) {
  const transport = String(value ?? '').toLowerCase();
  if (['ws', 'websocket'].includes(transport)) return 'websocket';
  if (['wt', 'quic', 'webtransport'].includes(transport)) return 'webtransport';
  if (['rtc', 'webrtc', 'p2p'].includes(transport)) return 'webrtc';
  throw new Error('Use --transport websocket, webtransport ou webrtc.');
}

function normalizePath(value) {
  const label = String(value ?? 'localhost').toLowerCase();
  if (['localhost', 'quick-tunnel', 'named-tunnel'].includes(label)) return label;
  throw new Error('Use --path localhost, quick-tunnel ou named-tunnel.');
}

function safeName(value) {
  return String(value)
    .replaceAll(/[^a-z0-9_.-]+/gi, '-')
    .replaceAll(/^-|-$/g, '');
}

function timestampName() {
  return new Date().toISOString().replaceAll(/[:.]/g, '-');
}

class CdpConnection {
  constructor(url) {
    this.url = url;
    this.socket = null;
    this.nextId = 1;
    this.pending = new Map();
  }

  async connect() {
    this.socket = new WebSocket(this.url);
    await new Promise((resolve, reject) => {
      const opened = () => {
        cleanup();
        resolve();
      };
      const failed = (event) => {
        cleanup();
        reject(event?.error ?? new Error(`Falha CDP em ${this.url}`));
      };
      const cleanup = () => {
        this.socket.removeEventListener('open', opened);
        this.socket.removeEventListener('error', failed);
      };
      this.socket.addEventListener('open', opened);
      this.socket.addEventListener('error', failed);
    });
    this.socket.addEventListener('message', (event) => this.onMessage(event));
    this.socket.addEventListener('close', () => this.onClose());
    return this;
  }

  onMessage(event) {
    const raw = typeof event.data === 'string' ? event.data : Buffer.from(event.data).toString();
    const message = JSON.parse(raw);
    if (!message.id) return;
    const pending = this.pending.get(message.id);
    if (!pending) return;
    this.pending.delete(message.id);
    clearTimeout(pending.timer);
    if (message.error) pending.reject(new Error(`${pending.method}: ${message.error.message}`));
    else pending.resolve(message.result ?? {});
  }

  onClose() {
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(new Error(`CDP fechou durante ${pending.method}`));
    }
    this.pending.clear();
  }

  send(method, params = {}, timeoutMs = 15_000) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`${method}: timeout`));
      }, timeoutMs);
      this.pending.set(id, { method, resolve, reject, timer });
      this.socket.send(JSON.stringify({ id, method, params }));
    });
  }

  async evaluate(expression) {
    const response = await this.send('Runtime.evaluate', {
      expression,
      awaitPromise: true,
      returnByValue: true,
    });
    if (response.exceptionDetails) {
      throw new Error(
        response.exceptionDetails.exception?.description ?? 'Runtime.evaluate falhou',
      );
    }
    return response.result?.value;
  }

  close() {
    if (this.socket?.readyState === WebSocket.OPEN) this.socket.close();
  }
}

async function fetchJson(url, timeoutMs = 3000) {
  const response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs), cache: 'no-store' });
  if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
  return response.json();
}

async function browserVersion(base) {
  return fetchJson(`${base}/json/version`);
}

async function listTargets(base) {
  return fetchJson(`${base}/json/list`);
}

function findChrome() {
  const candidates = [
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
    path.join(process.env.LOCALAPPDATA ?? '', 'Google', 'Chrome', 'Application', 'chrome.exe'),
  ];
  return candidates.find((candidate) => candidate && requireExists(candidate));
}

function requireExists(candidate) {
  try {
    return Boolean(process.getBuiltinModule('node:fs').statSync(candidate).isFile());
  } catch {
    return false;
  }
}

async function waitBrowser(base, timeoutMs = 12_000) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      return await browserVersion(base);
    } catch (error) {
      lastError = error;
      await sleep(150);
    }
  }
  throw lastError ?? new Error('Chrome nao publicou o endpoint CDP.');
}

async function ensureBrowser({
  port,
  headless,
  profileDir,
  autoSelectTitle = null,
  secureOrigin = null,
}) {
  const base = `http://127.0.0.1:${port}`;
  try {
    const version = await browserVersion(base);
    await fs.rm(profileDir, { recursive: true, force: true }).catch(() => {});
    return { base, version, launched: false, child: null, profileDir: null };
  } catch {
    // A porta livre e o caso normal: o ensaio abre um perfil dedicado abaixo.
  }

  const chrome = findChrome();
  if (!chrome) throw new Error('Google Chrome nao encontrado nos caminhos padrao.');
  await fs.mkdir(profileDir, { recursive: true });
  const args = [
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${profileDir}`,
    '--no-first-run',
    '--no-default-browser-check',
    '--disable-background-networking',
    '--new-window',
  ];
  if (autoSelectTitle) args.push(`--auto-select-tab-capture-source-by-title=${autoSelectTitle}`);
  if (secureOrigin) args.push(`--unsafely-treat-insecure-origin-as-secure=${secureOrigin}`);
  if (headless) args.push('--headless=new');
  args.push('about:blank');
  const child = spawn(chrome, args, {
    detached: false,
    stdio: 'ignore',
    windowsHide: Boolean(headless),
  });
  const version = await waitBrowser(base);
  return { base, version, launched: true, child, profileDir };
}

async function connectBrowser(browser) {
  return new CdpConnection(browser.version.webSocketDebuggerUrl).connect();
}

async function connectTarget(base, targetId) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    const target = (await listTargets(base)).find((entry) => entry.id === targetId);
    if (target?.webSocketDebuggerUrl)
      return new CdpConnection(target.webSocketDebuggerUrl).connect();
    await sleep(100);
  }
  throw new Error(`Target CDP nao apareceu: ${targetId}`);
}

async function createPage(
  browserCdp,
  browserBase,
  url = 'about:blank',
  preload = null,
  browserContextId = null,
) {
  const { targetId } = await browserCdp.send('Target.createTarget', {
    url: 'about:blank',
    ...(browserContextId ? { browserContextId } : {}),
  });
  const page = await connectTarget(browserBase, targetId);
  await page.send('Page.enable');
  await page.send('Runtime.enable');
  if (preload) await page.send('Page.addScriptToEvaluateOnNewDocument', { source: preload });
  if (url === 'about:blank') return { id: targetId, cdp: page, url };

  await page.send('Page.navigate', { url });
  const expectedOrigin = new URL(url).origin;
  const deadline = Date.now() + 10_000;
  let last = null;
  while (Date.now() < deadline) {
    try {
      last = await page.evaluate(
        `({ ready: document.readyState, href: location.href, origin: location.origin, secure: isSecureContext })`,
      );
      if (isExpectedPageReady(last, expectedOrigin)) {
        return { id: targetId, cdp: page, url, navigation: last };
      }
    } catch {
      // O contexto pode ser destruido entre about:blank e a pagina pedida.
    }
    await sleep(100);
  }
  throw new Error(
    `contexto-inseguro-ou-pagina-errada: esperado=${expectedOrigin}; observado=${JSON.stringify(last)}`,
  );
}

async function labelPage(page, title, role) {
  await page.evaluate(`(() => {
    document.title = ${JSON.stringify(title)};
    document.documentElement.dataset.ensaioRole = ${JSON.stringify(role)};
  })()`);
}

function transportPreload(transport) {
  const disabled = [];
  if (transport === 'websocket') disabled.push('WebTransport', 'RTCPeerConnection');
  if (transport === 'webtransport') disabled.push('RTCPeerConnection');
  return `(() => {
    const disabled = ${JSON.stringify(disabled)};
    for (const name of disabled) {
      try { Object.defineProperty(globalThis, name, { value: undefined, configurable: true }); } catch {}
    }
    Object.defineProperty(globalThis, '__ENSAIO_TRANSPORT', { value: ${JSON.stringify(transport)} });
  })();`;
}

function sourceCapturePreload(runId) {
  return `(() => {
    const runId = ${JSON.stringify(runId)};
    const install = () => {
      const media = navigator.mediaDevices;
      const original = media?.getDisplayMedia;
      if (typeof original !== 'function') {
        globalThis.__ENSAIO_CAPTURE_PROBE = {
          runId,
          status: 'contexto-inseguro-ou-pagina-errada',
          installedAt: Date.now(),
        };
        return false;
      }
      if (original.__ensaioCaptureProbe) return true;
      const wrapped = async (...args) => {
        globalThis.__ENSAIO_CAPTURE_PROBE = { runId, status: 'pending', calledAt: Date.now() };
        try {
          const stream = await original.apply(media, args);
          const track = stream.getVideoTracks()[0];
          globalThis.__ENSAIO_CAPTURE_PROBE = {
            runId,
            status: 'resolved',
            resolvedAt: Date.now(),
            label: track?.label ?? '',
            settings: track?.getSettings?.() ?? null,
          };
          globalThis.__ENSAIO_CAPTURE_STREAM = stream;
          return stream;
        } catch (error) {
          globalThis.__ENSAIO_CAPTURE_PROBE = {
            runId,
            status: 'rejected',
            rejectedAt: Date.now(),
            name: error?.name,
            message: error?.message,
          };
          throw error;
        }
      };
      Object.defineProperty(wrapped, '__ensaioCaptureProbe', { value: true });
      Object.defineProperty(media, 'getDisplayMedia', { value: wrapped, configurable: true });
      globalThis.__ENSAIO_CAPTURE_PROBE = { runId, status: 'installed', installedAt: Date.now() };
      return true;
    };
    if (!install()) addEventListener('DOMContentLoaded', install, { once: true });
  })();`;
}

const PATTERN_SOURCE = `(() => {
  document.title = 'ENSAIO RELOGIO VISUAL';
  document.documentElement.style.cssText = 'margin:0;background:#07111f;overflow:hidden';
  document.body.style.cssText = 'margin:0;overflow:hidden;background:#07111f';
  document.body.innerHTML = '<canvas id="ensaio-clock"></canvas>';
  const canvas = document.getElementById('ensaio-clock');
  const visibleCtx = canvas.getContext('2d', { alpha: false });
  const frame = document.createElement('canvas');
  const ctx = frame.getContext('2d', { alpha: false });
  let delayMs = 0;
  const crc8 = (bytes) => {
    let crc = 0;
    for (const byte of bytes) {
      crc ^= byte;
      for (let bit = 0; bit < 8; bit++) crc = crc & 0x80 ? ((crc << 1) ^ 0x07) & 0xff : (crc << 1) & 0xff;
    }
    return crc;
  };
  const bitsOf = (timestamp) => {
    const n = BigInt(Math.max(0, Math.round(timestamp)));
    const bytes = new Uint8Array(8);
    bytes[0] = 0xd1;
    for (let i = 0; i < 6; i++) bytes[6 - i] = Number((n >> BigInt(i * 8)) & 0xffn);
    bytes[7] = crc8(bytes.subarray(0, 7));
    const bits = [];
    for (const byte of bytes) for (let bit = 7; bit >= 0; bit--) bits.push((byte >> bit) & 1);
    return bits;
  };
  const resize = () => {
    canvas.width = Math.max(640, innerWidth);
    canvas.height = Math.max(360, innerHeight);
    frame.width = canvas.width;
    frame.height = canvas.height;
  };
  addEventListener('resize', resize);
  resize();
  const draw = () => {
    const w = canvas.width;
    const h = canvas.height;
    const now = Date.now();
    const encodedAt = now - delayMs;
    const bits = bitsOf(encodedAt);
    ctx.fillStyle = '#07111f';
    ctx.fillRect(0, 0, w, h);
    const gx = w * 0.05;
    const gy = h * 0.05;
    const gw = w * 0.9;
    const gh = h * 0.35;
    const cw = gw / 16;
    const ch = gh / 4;
    for (let i = 0; i < bits.length; i++) {
      const col = i % 16;
      const row = Math.floor(i / 16);
      ctx.fillStyle = bits[i] ? '#ffffff' : '#000000';
      ctx.fillRect(gx + col * cw, gy + row * ch, cw + 0.5, ch + 0.5);
    }
    ctx.fillStyle = '#45d6ff';
    ctx.font = Math.max(24, Math.round(h * 0.075)) + 'px system-ui, sans-serif';
    ctx.fillText('DISCORD LOCUTOR - RELOGIO DO ENSAIO', w * 0.05, h * 0.52);
    ctx.fillStyle = '#ffffff';
    ctx.font = Math.max(32, Math.round(h * 0.12)) + 'px ui-monospace, monospace';
    ctx.fillText(new Date(encodedAt).toISOString(), w * 0.05, h * 0.7);
    ctx.font = Math.max(20, Math.round(h * 0.055)) + 'px system-ui, sans-serif';
    ctx.fillText('delay de controle: ' + delayMs + ' ms', w * 0.05, h * 0.82);
    // A captura da aba pode compor o canvas entre dois fillRect. Publicar o
    // quadro inteiro num unico blit impede timestamp e CRC de frames diferentes.
    visibleCtx.drawImage(frame, 0, 0);
    requestAnimationFrame(draw);
  };
  globalThis.ensaioClock = {
    setDelay(value) { delayMs = Math.max(0, Number(value) || 0); return delayMs; },
    getDelay() { return delayMs; },
  };
  draw();
})();`;

const VIEWER_SAMPLE_SOURCE = `(() => {
  const crc8 = (bytes) => {
    let crc = 0;
    for (const byte of bytes) {
      crc ^= byte;
      for (let bit = 0; bit < 8; bit++) crc = crc & 0x80 ? ((crc << 1) ^ 0x07) & 0xff : (crc << 1) & 0xff;
    }
    return crc;
  };
  const decode = (source) => {
    const width = source instanceof HTMLVideoElement ? source.videoWidth : source.width;
    const height = source instanceof HTMLVideoElement ? source.videoHeight : source.height;
    if (!width || !height) return null;
    // Uma leitura por celula pode atravessar a troca de frame do <video> e
    // misturar timestamp/CRC. Congelar o quadro uma vez torna as 64 amostras
    // uma observacao atomica.
    const frozen = document.createElement('canvas');
    frozen.width = width;
    frozen.height = height;
    frozen.getContext('2d', { alpha: false }).drawImage(source, 0, 0, width, height);
    const scratch = document.createElement('canvas');
    scratch.width = 16;
    scratch.height = 4;
    const ctx = scratch.getContext('2d', { alpha: false, willReadFrequently: true });
    const gx = width * 0.05;
    const gy = height * 0.05;
    const cw = (width * 0.9) / 16;
    const ch = (height * 0.35) / 4;
    for (let row = 0; row < 4; row++) {
      for (let col = 0; col < 16; col++) {
        const sx = gx + (col + 0.35) * cw;
        const sy = gy + (row + 0.35) * ch;
        ctx.drawImage(frozen, sx, sy, cw * 0.3, ch * 0.3, col, row, 1, 1);
      }
    }
    const rgba = ctx.getImageData(0, 0, 16, 4).data;
    const bits = [];
    for (let i = 0; i < 64; i++) {
      const at = i * 4;
      bits.push((rgba[at] + rgba[at + 1] + rgba[at + 2]) / 3 >= 128 ? 1 : 0);
    }
    const bytes = new Uint8Array(8);
    for (let i = 0; i < bits.length; i++) bytes[Math.floor(i / 8)] |= bits[i] << (7 - (i % 8));
    if (bytes[0] !== 0xd1 || crc8(bytes.subarray(0, 7)) !== bytes[7]) return null;
    let timestamp = 0n;
    for (let i = 1; i <= 6; i++) timestamp = (timestamp << 8n) | BigInt(bytes[i]);
    const value = Number(timestamp);
    return Number.isSafeInteger(value) ? value : null;
  };
  const visible = (element) => {
    const rect = element.getBoundingClientRect();
    return rect.width > 2 && rect.height > 2 && getComputedStyle(element).visibility !== 'hidden';
  };
  const candidates = [...document.querySelectorAll('canvas, video')].filter((element) => {
    if (!visible(element)) return false;
    if (element instanceof HTMLVideoElement) return element.readyState >= 2 && element.videoWidth > 0;
    return element.width > 0 && element.height > 0;
  });
  let timestampMs = null;
  let kind = null;
  let decodeError = null;
  for (const source of candidates) {
    try {
      timestampMs = decode(source);
      if (timestampMs !== null) { kind = source.tagName.toLowerCase(); break; }
    } catch (error) {
      decodeError = error.message;
    }
  }
  const text = (id) => document.getElementById(id)?.textContent?.trim() ?? '';
  const numberOf = (value) => {
    const match = String(value).match(/-?\\d+(?:[.,]\\d+)?/);
    return match ? Number(match[0].replace(',', '.')) : null;
  };
  const transport = text('pVia');
  const arrival = numberOf(text('pLag'));
  const qualityBadge = document.getElementById('qualityBadge');
  const optionalDatasetNumber = (name) => {
    const value = qualityBadge?.dataset?.[name];
    return value === undefined || value === null || value === '' ? null : Number(value);
  };
  return {
    valid: timestampMs !== null,
    timestampMs,
    visualLagMs: timestampMs === null ? null : Date.now() - timestampMs,
    sourceKind: kind,
    decodeError,
    transport,
    arrivalLagMs: /WebRTC/i.test(transport) ? null : arrival,
    rtcRttMs: /WebRTC/i.test(transport) ? arrival : null,
    jitterMs: numberOf(text('pJitter')),
    frames: numberOf(text('pFps')),
    resolution: text('pRes'),
    qualityGateState: qualityBadge?.dataset?.state ?? null,
    bitrateBps: optionalDatasetNumber('bitrateBps'),
    packetLossPct: optionalDatasetNumber('packetLossPct'),
    droppedFramesPct: optionalDatasetNumber('droppedFramesPct'),
  };
})()`;

async function installPattern(page, title = 'ENSAIO RELOGIO VISUAL') {
  await page.evaluate(PATTERN_SOURCE);
  const installed = await page.evaluate(`(() => {
    document.title = ${JSON.stringify(title)};
    return { title: document.title, clock: Boolean(globalThis.ensaioClock) };
  })()`);
  if (installed?.title !== title || !installed?.clock) {
    throw new Error(`Relogio visual nao instalou: ${JSON.stringify(installed)}`);
  }
  await sleep(300);
}

async function setPatternDelay(page, delayMs) {
  return page.evaluate(`globalThis.ensaioClock?.setDelay(${Number(delayMs)})`);
}

async function readViewer(page) {
  try {
    return await page.evaluate(VIEWER_SAMPLE_SOURCE);
  } catch (error) {
    return { valid: false, visualLagMs: null, decodeError: error.message };
  }
}

class SystemMonitor {
  constructor() {
    this.previousCpu = os.cpus();
    this.lastProcessesAt = 0;
    this.processCounts = {};
    this.processUpdate = null;
  }

  cpuPercent() {
    const current = os.cpus();
    let totalDelta = 0;
    let idleDelta = 0;
    for (let index = 0; index < current.length; index++) {
      const before = this.previousCpu[index]?.times ?? current[index].times;
      const after = current[index].times;
      const beforeTotal = Object.values(before).reduce((sum, value) => sum + value, 0);
      const afterTotal = Object.values(after).reduce((sum, value) => sum + value, 0);
      totalDelta += afterTotal - beforeTotal;
      idleDelta += after.idle - before.idle;
    }
    this.previousCpu = current;
    return totalDelta > 0 ? Math.max(0, Math.min(100, (1 - idleDelta / totalDelta) * 100)) : 0;
  }

  updateProcesses(now) {
    if (now - this.lastProcessesAt < PROCESS_EVERY_MS || this.processUpdate) return;
    this.lastProcessesAt = now;
    this.processUpdate = execFileAsync('tasklist.exe', ['/FO', 'CSV', '/NH'], {
      windowsHide: true,
      timeout: 5000,
      maxBuffer: 4 * 1024 * 1024,
    })
      .then(({ stdout }) => {
        const wanted = new Set([
          'node.exe',
          'codex.exe',
          'claude.exe',
          'chrome.exe',
          'cloudflared.exe',
        ]);
        const counts = Object.fromEntries([...wanted].map((name) => [name.slice(0, -4), 0]));
        for (const line of stdout.split(/\r?\n/)) {
          const match = line.match(/^"([^"]+)"/);
          const name = match?.[1]?.toLowerCase();
          if (wanted.has(name)) counts[name.slice(0, -4)]++;
        }
        this.processCounts = counts;
      })
      .catch((error) => {
        this.processCounts = { error: error.message };
      })
      .finally(() => {
        this.processUpdate = null;
      });
  }

  sample() {
    const now = Date.now();
    this.updateProcesses(now);
    return {
      cpuPercent: this.cpuPercent(),
      freeMemoryBytes: os.freemem(),
      processCounts: { ...this.processCounts },
    };
  }
}

class TunnelProbe {
  constructor(origin, pathLabel) {
    this.origin = origin;
    this.enabled = pathLabel !== 'localhost';
    this.lastAt = 0;
    this.pending = null;
    this.last = { tunnelOk: this.enabled ? null : undefined, tunnelRttMs: null, tunnelError: null };
  }

  async refresh() {
    const started = performance.now();
    try {
      const response = await fetch(`${this.origin}/api/transports?ensaio=${Date.now()}`, {
        cache: 'no-store',
        signal: AbortSignal.timeout(3000),
      });
      this.last = {
        tunnelOk: response.ok,
        tunnelRttMs: performance.now() - started,
        tunnelError: response.ok ? null : `HTTP ${response.status}`,
      };
    } catch (error) {
      this.last = {
        tunnelOk: false,
        tunnelRttMs: performance.now() - started,
        tunnelError: error.message,
      };
    }
  }

  sample() {
    if (!this.enabled) return this.last;
    const now = Date.now();
    if (now - this.lastAt >= PROCESS_EVERY_MS && !this.pending) {
      this.lastAt = now;
      this.pending = this.refresh().finally(() => {
        this.pending = null;
      });
    }
    return this.last;
  }
}

const QUALITY_SAMPLE_SOURCE = `(() => {
  const selectors = ['#tela-auto', '#camera-auto', '[data-quality-auto]'];
  const notices = selectors.flatMap((selector) => [...document.querySelectorAll(selector)]).map((element) => ({
    selector: element.id ? '#' + element.id : '[data-quality-auto]',
    visible: !element.hidden && Boolean(element.textContent.trim()),
    text: element.textContent.trim(),
  }));
  let stored = null;
  try { stored = JSON.parse(localStorage.getItem('ajustes') ?? 'null'); } catch {}
  const inActivity = Boolean(document.querySelector('[data-quality-auto]'));
  const bitrate = Number(
    document.getElementById('qualidade')?.value ?? (inActivity ? stored?.bitrate ?? 2500000 : NaN),
  );
  const fps = Number(document.getElementById('quadros')?.value ?? (inActivity ? stored?.fps ?? 30 : NaN));
  return {
    title: document.title,
    url: location.href,
    notices,
    ceilingBitrate: Number.isFinite(bitrate) ? bitrate : null,
    ceilingFps: Number.isFinite(fps) ? fps : null,
  };
})()`;

class QualityProbe {
  constructor(browserBase, excludedTargetIds = []) {
    this.browserBase = browserBase;
    this.excluded = new Set(excludedTargetIds);
    this.lastAt = 0;
    this.pending = null;
    this.last = {
      visible: false,
      text: '',
      ceilingBitrate: null,
      ceilingFps: null,
      targetId: null,
    };
  }

  async refresh() {
    const snapshots = [];
    for (const target of await listTargets(this.browserBase)) {
      if (target.type !== 'page' || this.excluded.has(target.id) || !target.webSocketDebuggerUrl)
        continue;
      const page = await new CdpConnection(target.webSocketDebuggerUrl).connect();
      try {
        const value = await page.evaluate(QUALITY_SAMPLE_SOURCE);
        if (value?.notices?.length) snapshots.push({ ...value, targetId: target.id });
      } catch {
        // Pagina navegando ou de outra origem; ela simplesmente nao e a origem.
      } finally {
        page.close();
      }
    }
    const visible = snapshots.find((snapshot) => snapshot.notices.some((notice) => notice.visible));
    const withCeiling = snapshots.find(
      (snapshot) =>
        Number.isFinite(snapshot.ceilingBitrate) && Number.isFinite(snapshot.ceilingFps),
    );
    const chosen = visible ?? withCeiling ?? snapshots[0];
    this.last = chosen
      ? {
          visible: chosen.notices.some((notice) => notice.visible),
          text: chosen.notices.find((notice) => notice.visible)?.text ?? '',
          ceilingBitrate: chosen.ceilingBitrate,
          ceilingFps: chosen.ceilingFps,
          targetId: chosen.targetId,
        }
      : { visible: false, text: '', ceilingBitrate: null, ceilingFps: null, targetId: null };
    return this.last;
  }

  async sample(force = false) {
    const now = Date.now();
    if (force) {
      this.lastAt = now;
      return this.refresh();
    }
    if (now - this.lastAt >= PROCESS_EVERY_MS && !this.pending) {
      this.lastAt = now;
      this.pending = this.refresh().finally(() => {
        this.pending = null;
      });
    }
    return this.last;
  }
}

async function waitQualityCeiling(quality, timeoutMs = 120_000) {
  const deadline = Date.now() + timeoutMs;
  let cleanReads = 0;
  let last = null;
  while (Date.now() < deadline) {
    last = await quality.sample(true);
    const ceilingOk =
      last.ceilingBitrate === null || (last.ceilingBitrate === 2_500_000 && last.ceilingFps === 30);
    if (!last.visible && ceilingOk) cleanReads++;
    else cleanReads = 0;
    if (cleanReads >= 2) return { ready: true, snapshot: last };
    await sleep(1000);
  }
  return { ready: false, snapshot: last };
}

async function waitViewerClean(page, timeoutMs = 90_000) {
  const deadline = Date.now() + timeoutMs;
  let cleanReads = 0;
  let last = null;
  while (Date.now() < deadline) {
    last = await readViewer(page);
    const clean =
      last.valid &&
      Number.isFinite(last.visualLagMs) &&
      last.visualLagMs <= 750 &&
      Number.isFinite(last.frames) &&
      last.frames >= 20 &&
      last.qualityGateState === 'pass';
    cleanReads = clean ? cleanReads + 1 : 0;
    if (cleanReads >= 5) return { ready: true, snapshot: last };
    await sleep(1000);
  }
  return { ready: false, snapshot: last };
}

async function setNetworkConditions(page, conditions = null, { p2p = false } = {}) {
  if (impairmentBackend === 'wsl-netem' && (!p2p || netemHandlesP2p)) {
    const clear = async () => {
      try {
        await execFileAsync('wsl.exe', [
          '-d',
          'eLxr',
          '-u',
          'root',
          '--',
          'tc',
          'qdisc',
          'del',
          'dev',
          'eth0',
          'root',
        ]);
      } catch {
        // Ausencia de qdisc e o estado limpo desejado.
      }
    };
    const latency = Math.max(0, Number(conditions?.latency ?? 0));
    const throughput = Number(conditions?.downloadThroughput ?? -1);
    const packetLoss = Math.max(0, Number(conditions?.packetLoss ?? 0));
    if (!conditions || (latency === 0 && throughput <= 0 && packetLoss === 0)) {
      await clear();
      return;
    }
    const netem = [
      '-d',
      'eLxr',
      '-u',
      'root',
      '--',
      'tc',
      'qdisc',
      'replace',
      'dev',
      'eth0',
      'root',
      'netem',
    ];
    if (latency > 0) netem.push('delay', `${latency}ms`);
    if (packetLoss > 0) netem.push('loss', `${packetLoss}%`);
    if (throughput > 0) netem.push('rate', `${Math.round(throughput * 8)}bit`);
    await execFileAsync('wsl.exe', netem);
    return;
  }
  await page.send('Network.enable');
  if (p2p) {
    await page.send('Network.emulateNetworkConditions', {
      offline: false,
      latency: Number(conditions?.latency ?? 0),
      downloadThroughput: Number(conditions?.downloadThroughput ?? -1),
      uploadThroughput: Number(conditions?.uploadThroughput ?? -1),
      packetLoss: Number(conditions?.packetLoss ?? 0),
      packetQueueLength: Number(conditions?.packetQueueLength ?? 0),
      packetReordering: Boolean(conditions?.packetReordering),
    });
    return;
  }
  const matchedNetworkConditions = conditions
    ? [
        {
          urlPattern: '',
          latency: Number(conditions.latency ?? 0),
          downloadThroughput: Number(conditions.downloadThroughput ?? -1),
          uploadThroughput: Number(conditions.uploadThroughput ?? -1),
          packetLoss: Number(conditions.packetLoss ?? 0),
          packetQueueLength: Number(conditions.packetQueueLength ?? 0),
          packetReordering: Boolean(conditions.packetReordering),
          offline: false,
        },
      ]
    : [];
  await page.send('Network.emulateNetworkConditionsByRule', {
    offline: false,
    matchedNetworkConditions,
  });
}

function commonConditions(latency = 250, transport = 'websocket') {
  return {
    latency,
    downloadThroughput: 75_000,
    uploadThroughput: -1,
    packetLoss: 5,
    ...(transport === 'webrtc' ? { packetQueueLength: 20, packetReordering: true } : {}),
  };
}

function conditionEvidence(conditions = null) {
  return {
    configuredLatencyMs: Number(conditions?.latency ?? 0),
    configuredThroughputBps:
      Number(conditions?.downloadThroughput ?? -1) > 0
        ? Number(conditions.downloadThroughput) * 8
        : null,
    configuredPacketLossPct: Number(conditions?.packetLoss ?? 0),
    impairmentBackend,
  };
}

async function readWslNetemStats() {
  if (impairmentBackend !== 'wsl-netem') return null;
  try {
    const { stdout } = await execFileAsync('wsl.exe', [
      '-d',
      'eLxr',
      '-u',
      'root',
      '--',
      'tc',
      '-s',
      'qdisc',
      'show',
      'dev',
      'eth0',
    ]);
    const qdisc = stdout.match(/^qdisc netem[^\r\n]*/m)?.[0] ?? null;
    const counters = stdout.match(/Sent \d+ bytes (\d+) pkt \(dropped (\d+)/);
    const sentPackets = counters ? Number(counters[1]) : null;
    const droppedPackets = counters ? Number(counters[2]) : null;
    return {
      qdisc,
      sentPackets,
      droppedPackets,
      actualLossPct:
        Number.isFinite(sentPackets) && Number.isFinite(droppedPackets) && sentPackets > 0
          ? (droppedPackets / (sentPackets + droppedPackets)) * 100
          : null,
    };
  } catch (error) {
    return { error: error.message };
  }
}

async function sampleFor({
  viewer,
  durationMs,
  phase,
  runStartedAt,
  monitor,
  tunnel,
  quality,
  extra = {},
  onTick = null,
}) {
  const records = [];
  const phaseStarted = performance.now();
  let nextAt = phaseStarted;
  while (performance.now() - phaseStarted < durationMs) {
    const elapsed = performance.now() - phaseStarted;
    await onTick?.(elapsed);
    const [media, load, tunnelState, qualityState] = await Promise.all([
      readViewer(viewer),
      monitor.sample(),
      tunnel.sample(),
      quality?.sample() ?? null,
    ]);
    records.push({
      at: new Date().toISOString(),
      tMs: performance.now() - runStartedAt,
      phase,
      valid: Boolean(media.valid),
      visualLagMs: media.visualLagMs,
      arrivalLagMs: media.arrivalLagMs,
      rtcRttMs: media.rtcRttMs,
      jitterMs: media.jitterMs,
      frames: media.frames,
      bitrateBps: media.bitrateBps,
      packetLossPct: media.packetLossPct,
      droppedFramesPct: media.droppedFramesPct,
      qualityGateState: media.qualityGateState,
      transportObserved: media.transport,
      resolution: media.resolution,
      decodeError: media.decodeError,
      cpuPercent: load.cpuPercent,
      freeMemoryBytes: load.freeMemoryBytes,
      processCounts: load.processCounts,
      ...tunnelState,
      qualityVisible: qualityState?.visible ?? null,
      qualityText: qualityState?.text ?? '',
      qualityCeilingBitrate: qualityState?.ceilingBitrate ?? null,
      qualityCeilingFps: qualityState?.ceilingFps ?? null,
      ...extra,
    });
    nextAt += SAMPLE_EVERY_MS;
    await sleep(Math.max(0, nextAt - performance.now()));
  }
  return records;
}

function expectedTransport(transport) {
  return {
    websocket: /WebSocket/i,
    webtransport: /WebTransport/i,
    webrtc: /WebRTC/i,
  }[transport];
}

async function waitForManualReady({ rl, source, viewer, runId, transport }) {
  const expected = expectedTransport(transport);
  while (true) {
    await rl.question('\nQuando a aba do espectador estiver exibindo o relogio, pressione Enter: ');
    const [sourceCapture, observed] = await Promise.all([
      source.evaluate('globalThis.__ENSAIO_CAPTURE_PROBE ?? null'),
      readViewer(viewer),
    ]);
    const sourceReady = sourceCapture?.status === 'resolved';
    const viewerReady = observed.valid && expected.test(observed.transport ?? '');
    if (sourceReady && viewerReady) return { sourceCapture, observed };

    console.log('\nAINDA NAO PRONTO — a rodada continua aberta, nada foi medido.');
    console.log(
      `  origem: ${sourceCapture?.status ?? 'sem probe'}; captura: ${sourceCapture?.label || 'nao observada'}`,
    );
    console.log(
      `  espectador: relogio=${observed.valid ? 'sim' : 'nao'}; transporte=${observed.transport ?? '-'}`,
    );
    console.log(
      `  conclua ORIGEM ${runId} -> compartilhe o relogio -> ESPECTADOR ${runId}; depois tente Enter novamente.`,
    );
  }
}

async function waitForAutomaticReady({ source, viewer, runId, transport, timeoutMs = 60_000 }) {
  const expected = expectedTransport(transport);
  const deadline = Date.now() + timeoutMs;
  let last = null;
  while (Date.now() < deadline) {
    const [sourceCapture, observed] = await Promise.all([
      source.evaluate('globalThis.__ENSAIO_CAPTURE_PROBE ?? null'),
      readViewer(viewer),
    ]);
    last = { sourceCapture, observed };
    if (
      sourceCapture?.runId === runId &&
      sourceCapture?.status === 'resolved' &&
      observed.valid &&
      expected.test(observed.transport)
    ) {
      return last;
    }
    await sleep(250);
  }
  throw new Error(`Automacao nao deixou origem/espectador prontos: ${JSON.stringify(last)}`);
}

async function calibrateVisualMeter({
  viewer,
  pattern,
  monitor,
  tunnel,
  quality,
  plateauMs = 5000,
}) {
  await setNetworkConditions(viewer, null);
  const runStartedAt = performance.now();
  const records = [];
  for (const delayMs of CLOCK_DELAYS) {
    await setPatternDelay(pattern, delayMs);
    await sleep(500);
    records.push(
      ...(await sampleFor({
        viewer,
        durationMs: plateauMs,
        phase: 'calibracao-medidor',
        runStartedAt,
        monitor,
        tunnel,
        quality,
        extra: { configuredDelayMs: delayMs },
      })),
    );
  }
  await setPatternDelay(pattern, 0);
  return { records, evaluation: evaluateCalibration(records) };
}

async function calibrateImpairment({
  viewer,
  source,
  pattern,
  monitor,
  tunnel,
  quality,
  transport,
  plateauMs = 5000,
}) {
  await setPatternDelay(pattern, 0);
  const impairmentTarget = transport === 'webrtc' ? source : viewer;
  const delays = transport === 'webtransport' ? WEBTRANSPORT_IMPAIRMENT_DELAYS : IMPAIRMENT_DELAYS;
  const runStartedAt = performance.now();
  const records = [];
  try {
    for (const delayMs of delays) {
      await setNetworkConditions(
        impairmentTarget,
        {
          latency: delayMs,
          downloadThroughput: -1,
          uploadThroughput: -1,
        },
        { p2p: transport === 'webrtc' },
      );
      await sleep(1000);
      records.push(
        ...(await sampleFor({
          viewer,
          durationMs: plateauMs,
          phase: 'calibracao-impairment',
          runStartedAt,
          monitor,
          tunnel,
          quality,
          extra: { configuredDelayMs: delayMs },
        })),
      );
    }
  } finally {
    await setNetworkConditions(impairmentTarget, null, { p2p: transport === 'webrtc' });
  }
  return {
    records,
    evaluation: evaluateCalibration(
      records,
      transport === 'webtransport'
        ? { finalMinimum: 250, stepMinimum: 70 }
        : { finalMinimum: CRITERIOS.impairmentFinalMinMs },
    ),
  };
}

async function runQualityTrial({ viewer, transport, quality }) {
  if (transport === 'webrtc') {
    return { status: 'N/A', reason: 'relay suspende o laco quando todo espectador esta em WebRTC' };
  }

  await setNetworkConditions(viewer, null);
  const ceilingReady = await waitQualityCeiling(quality);
  if (!ceilingReady.ready) {
    return {
      status: 'INCONCLUSIVO',
      reason: 'qualidade-nao-voltou-ao-teto-antes-do-ensaio',
      initial: ceilingReady.snapshot,
    };
  }
  const initial = await quality.sample(true);
  if (initial.ceilingBitrate !== 2_500_000 || initial.ceilingFps !== 30) {
    return {
      status: 'INCONCLUSIVO',
      reason: 'teto-manual-nao-observado',
      initial,
      expected: { ceilingBitrate: 2_500_000, ceilingFps: 30 },
    };
  }

  const startedAt = performance.now();
  const events = [];
  let previous = initial;
  let firstDownAt = null;
  let restoredAt = null;
  let firstUpAt = null;
  try {
    await setNetworkConditions(viewer, commonConditions(250, transport));
    const downDeadline = startedAt + 12_000;
    while (performance.now() < downDeadline) {
      const current = await quality.sample(true);
      const atMs = performance.now() - startedAt;
      if (current.visible !== previous.visible || current.text !== previous.text) {
        events.push({ atMs, kind: current.visible ? 'down' : 'up', ...current });
      }
      if (current.visible && firstDownAt === null) {
        firstDownAt = atMs;
        break;
      }
      previous = current;
      await sleep(250);
    }

    if (firstDownAt === null) {
      return { status: 'FAIL', reason: 'quality-down-nao-chegou-em-12s', events, initial };
    }
    await sleep(1000);
    await setNetworkConditions(viewer, null);
    restoredAt = performance.now() - startedAt;
    previous = await quality.sample(true);
    const recoveryDeadline = performance.now() + 18_000;
    while (performance.now() < recoveryDeadline) {
      const current = await quality.sample(true);
      const atMs = performance.now() - startedAt;
      if (current.visible !== previous.visible || current.text !== previous.text) {
        events.push({ atMs, kind: current.visible ? 'down' : 'up', ...current });
      }
      if (!current.visible && previous.visible && firstUpAt === null) firstUpAt = atMs;
      previous = current;
      await sleep(250);
    }
  } finally {
    await setNetworkConditions(viewer, null);
  }

  const newDownAfterRestore = events.some(
    (event) => event.kind === 'down' && event.atMs > restoredAt,
  );
  const downEvents = events.filter((event) => event.kind === 'down');
  const upEvents = events.filter((event) => event.kind === 'up');
  const spacing = (entries, minimum) =>
    entries.slice(1).every((entry, index) => entry.atMs - entries[index].atMs >= minimum);
  const checks = {
    downEm12s: firstDownAt !== null && firstDownAt <= 12_000,
    upNaoAntesDe10s: firstUpAt !== null && firstUpAt - firstDownAt >= 10_000,
    upAte18sDoRestore: firstUpAt !== null && firstUpAt - restoredAt <= 18_000,
    semNovoDownDepoisDoRestore: !newDownAfterRestore,
    downCooldown2s: spacing(downEvents, 2000),
    upCooldown10s: spacing(upEvents, 10_000),
    tetoManualPreservado: [initial, ...events].every(
      (entry) =>
        entry.ceilingBitrate === null ||
        (entry.ceilingBitrate <= 2_500_000 && entry.ceilingFps <= 30),
    ),
    voltouAoTeto: firstUpAt !== null && previous.visible === false,
  };
  return {
    status: Object.values(checks).every(Boolean) ? 'PASS' : 'FAIL',
    reason: Object.values(checks).every(Boolean)
      ? 'histerese-e-teto-atendidos'
      : 'criterio-de-histerese-violado',
    checks,
    firstDownAtMs: firstDownAt,
    restoredAtMs: restoredAt,
    firstUpAtMs: firstUpAt,
    events,
    initial,
  };
}

async function runDelaySeries({
  viewer,
  source,
  transport,
  variant,
  monitor,
  tunnel,
  quality,
  durations,
}) {
  const impairmentTarget = transport === 'webrtc' ? source : viewer;
  const runStartedAt = performance.now();
  const records = [];
  await setNetworkConditions(impairmentTarget, null, { p2p: transport === 'webrtc' });
  records.push(
    ...(await sampleFor({
      viewer,
      durationMs: durations.baseline,
      phase: 'baseline',
      runStartedAt,
      monitor,
      tunnel,
      quality,
      extra: { variant, ...conditionEvidence() },
    })),
  );

  if (variant === 'B') {
    const latencies = [50, 100, 150, 200, 250];
    const throughputs = [400_000, 250_000, 150_000, 100_000, 75_000];
    let applied = -1;
    const rampEvidence = { variant, ...conditionEvidence() };
    records.push(
      ...(await sampleFor({
        viewer,
        durationMs: durations.ramp,
        phase: 'ramp',
        runStartedAt,
        monitor,
        tunnel,
        quality,
        extra: rampEvidence,
        onTick: async (elapsed) => {
          const step = Math.min(latencies.length - 1, Math.floor(elapsed / 1000));
          if (step === applied) return;
          applied = step;
          const conditions = {
            ...commonConditions(latencies[step], transport),
            downloadThroughput: throughputs[step],
          };
          await setNetworkConditions(impairmentTarget, conditions, { p2p: transport === 'webrtc' });
          Object.assign(rampEvidence, conditionEvidence(conditions));
        },
      })),
    );
    await setNetworkConditions(impairmentTarget, commonConditions(250, transport), {
      p2p: transport === 'webrtc',
    });
  } else {
    records.push(
      ...(await sampleFor({
        viewer,
        durationMs: durations.ramp,
        phase: 'ramp',
        runStartedAt,
        monitor,
        tunnel,
        quality,
        extra: { variant, ...conditionEvidence() },
      })),
    );
  }

  records.push(
    ...(await sampleFor({
      viewer,
      durationMs: durations.sustain,
      phase: 'sustain',
      runStartedAt,
      monitor,
      tunnel,
      quality,
      extra: {
        variant,
        ...conditionEvidence(variant === 'B' ? commonConditions(250, transport) : null),
      },
    })),
  );
  if (variant === 'B' && impairmentBackend === 'wsl-netem') {
    const netem = await readWslNetemStats();
    Object.assign(records.at(-1), {
      netemQdisc: netem?.qdisc ?? null,
      netemSentPackets: netem?.sentPackets ?? null,
      netemDroppedPackets: netem?.droppedPackets ?? null,
      netemActualLossPct: netem?.actualLossPct ?? null,
      netemStatsError: netem?.error ?? null,
    });
  }
  await setNetworkConditions(impairmentTarget, null, { p2p: transport === 'webrtc' });
  records.push({
    at: new Date().toISOString(),
    tMs: performance.now() - runStartedAt,
    phase: 'restore',
    event: 'network-conditions-cleared',
    variant,
    ...conditionEvidence(),
  });
  records.push(
    ...(await sampleFor({
      viewer,
      durationMs: durations.recovery,
      phase: 'recovery',
      runStartedAt,
      monitor,
      tunnel,
      quality,
      extra: { variant, ...conditionEvidence() },
    })),
  );
  return records;
}

async function runWtGate(outputPath) {
  const startedAt = new Date().toISOString();
  let stdout;
  let stderr;
  let exitCode = 0;
  try {
    const executable = process.platform === 'win32' ? process.env.ComSpec || 'cmd.exe' : 'npm';
    const args =
      process.platform === 'win32'
        ? ['/d', '/s', '/c', 'npm run test:webtransport']
        : ['run', 'test:webtransport'];
    const result = await execFileAsync(executable, args, {
      cwd: ROOT,
      windowsHide: true,
      timeout: 180_000,
      maxBuffer: 32 * 1024 * 1024,
      env: process.env,
    });
    stdout = result.stdout;
    stderr = result.stderr;
  } catch (error) {
    stdout = error.stdout ?? '';
    stderr = error.stderr ?? error.message;
    exitCode = Number(error.code) || 1;
  }
  const combined = `${stdout}\n${stderr}`;
  const flake = combined.includes('[flake]');
  const state = exitCode === 0 && !flake ? 'verde' : 'vermelho';
  await fs.writeFile(
    outputPath,
    [
      `startedAt=${startedAt}`,
      `exitCode=${exitCode}`,
      `flake=${flake}`,
      `gate=${state}`,
      '',
      combined,
    ].join('\n'),
  );
  return { state, exitCode, flake, startedAt, outputPath: path.basename(outputPath) };
}

function overallStatus(series) {
  if (!series.length) return 'NAO-RODADO';
  if (series.some((entry) => entry.evaluation.status === 'INCONCLUSIVO')) return 'INCONCLUSIVO';
  return series.every((entry) => entry.evaluation.status === 'PASS') ? 'PASS' : 'FAIL';
}

function fmt(value, digits = 1) {
  return Number.isFinite(value) ? Number(value).toFixed(digits) : 'n/d';
}

function reportMarkdown(metadata, meterCalibration, impairmentCalibration, series, qualityResult) {
  const capture = metadata.captureSelection;
  const rows = series
    .map(
      (entry) =>
        `| ${entry.variant}${entry.repetition} | ${entry.gateBefore ?? 'n/a'} | ${entry.gateAfter ?? 'n/a'} | ${entry.evaluation.status}${entry.evaluation.confounds.length ? `/${entry.evaluation.confounds.join('+')}` : ''} | ${fmt(entry.evaluation.metrics.rollingPeak)} | ${fmt(entry.evaluation.metrics.sustainP95)} | ${fmt(entry.evaluation.metrics.slopeMsPerS, 2)} | ${fmt(entry.evaluation.metrics.recoveredInMs, 0)} |`,
    )
    .join('\n');
  return `# Ensaio de delay em rede real

## Distincao que evita uma conclusao falsa

**\`player.getLag()\` mede chegada, nao playout.** Ele e atualizado antes de decode/fila em
\`client/src/player.js:141\`; \`client/src/main.js:1307\` apenas exibe esse valor. O atraso de
display abaixo vem do timestamp codificado nos pixels efetivamente desenhados. A coluna de
espera usa \`lag visual - lag de chegada\` e e uma estimativa declarada, nao uma medicao inventada.

## Identidade da rodada

- inicio: ${metadata.startedAt}
- origin: ${metadata.origin}
- caminho: ${metadata.path}
- transporte contratado: ${metadata.transport}
- Chrome: ${metadata.chrome}
- runId: ${metadata.runId ?? 'nao-aplicavel'}
- fonte esperada: ${metadata.captureTitle ?? 'nao-aplicavel'}
- impairment: ${metadata.impairmentMethod}
- porta HTTP local contratada: 3100 (3001 pertence a faixa excluida 2979-3078 nesta maquina)
- QUIC publico por Quick Tunnel: fora de escopo; tunnel HTTPS/TCP nao encaminha UDP/QUIC 4443

## Selecao da fonte: automatizada e verificada

- modo: ${metadata.captureSelectionMode ?? 'nao-aplicavel nesta calibracao autonoma'}
- label observado: ${capture?.sourceCapture?.label ?? 'nao-observado'}
- superficie: ${capture?.sourceCapture?.settings?.displaySurface ?? 'nao-observada'}
- titulo escolhido no picker: ${capture?.pickerAudit?.selectedAccessibleName ?? 'nao-observado'}
- picker contem runId: **${capture?.evaluation?.pickerSelectionMatchesRunId ? 'PASS' : capture ? 'FAIL' : 'NAO-APLICAVEL'}**
- label contem runId: **${capture?.evaluation?.labelMatchesRunId ? 'PASS' : 'NAO-DISPONIVEL/OPAQUE'}**
- marcador visual chegou ao viewer: **${capture?.evaluation?.pixelMarkerDecoded ? 'PASS' : capture ? 'FAIL' : 'NAO-APLICAVEL'}**

O Chrome 151 devolve \`track.label\` opaco para captura de aba. A identidade da fonte usa o switch
de testes do Chromium \`auto-select-tab-capture-source-by-title\`, auditado pelo titulo exato, e ainda
exige \`displaySurface:'browser'\` e o marcador/CRC decodificado no viewer. Nenhuma dessas provas,
isoladamente, promove a rodada.

## Controles positivos

- relogio visual 0/500/1000/1500 ms: **${meterCalibration.evaluation.pass ? 'PASS' : 'FAIL'}**;
  delta final ${fmt(meterCalibration.evaluation.finalDelta)} ms.
- impairment ${metadata.impairmentBackend ?? 'cdp'} 0/400/800/1200 ms: **${impairmentCalibration?.evaluation.pass ? 'PASS' : impairmentCalibration ? 'FAIL' : 'NAO-RODADO'}**;
  delta final ${fmt(impairmentCalibration?.evaluation.finalDelta)} ms.

Sem os dois controles positivos, nenhum "nao acumulou" e aceito.

## Series oficiais

Resultado agregado: **${overallStatus(series)}**. Sao exigidas tres repeticoes por A/B; um resultado
bom nao e promovido por maioria. Falha que coincide com carga ou tunnel vira INCONCLUSIVO, nunca PASS.

| serie | gate antes | gate depois | resultado | pico mediana 1 s (ms) | p95 sustenta (ms) | slope (ms/s) | recuperou em (ms) |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
${rows || '| — | — | — | NAO-RODADO | — | — | — | — |'}

Para WebTransport, gate vermelho antes ou depois domina o resultado: a serie fica
\`INCONCLUSIVO/gate\`. O numero e preservado no CSV, mas nao vira evidencia do player.

## Histerese e teto do usuario

- resultado: **${qualityResult?.status ?? 'NAO-RODADO'}**
- motivo: ${qualityResult?.reason ?? '—'}
- primeiro down: ${fmt(qualityResult?.firstDownAtMs, 0)} ms
- restore: ${fmt(qualityResult?.restoredAtMs, 0)} ms
- primeiro up: ${fmt(qualityResult?.firstUpAtMs, 0)} ms

WebRTC direto aparece como N/A aqui por desenho: sem espectador relay-only o relay suspende o laco.

## Criterios fixados antes da execucao

- pico: maior mediana movel de 1 s <= 2000 ms; nenhuma amostra >= 2500 ms;
- sustentacao: p95 <= baseline p95 + 1000 ms, slope <= 10 ms/s e crescimento inicio/fim <= 200 ms;
- recuperacao: janela de 3 s volta a baseline p95 + 100 ms em ate 12 s e permanece em +200 ms;
- jitter: precisa subir pelo menos 20 ms para a rodada provar adaptacao; espera estimada <= 300 ms;
- host: CPU >= 90% ou memoria livre < 1 GiB por 2 s, quando coincide com falha, gera INCONCLUSIVO/carga;
- tunnel: falhas > 2% ou p95 > baseline p95 + 500 ms, quando coincide com falha, gera INCONCLUSIVO/tunel;
- repeticao: 3/3 por A/B, com mediana/IQR e CSV bruto.

## Limites declarados

${
  metadata.impairmentBackend === 'wsl-netem'
    ? '- O impairment usa `tc netem` no WSL atravessado pelos pacotes reais; os valores configurados ficam em cada linha dos CSVs.'
    : '- O impairment e sintetico no Chrome, embora o caminho medido seja real e etiquetado.'
}
- Descarte de frames velhos no cliente nao tem contador exposto. A prova e indireta: lag visual
  limitado, frames continuando e retorno ao piso. \`takeFrameCount()\` conta desenhados, nao descartados.
- Em duas maquinas, o relogio visual exige calibracao de offset; na mesma maquina ele e exato.
- Com duas abas locais, o label de tunnel no WebRTC descreve a sinalizacao; depois de P2P, a midia
  nao atravessa o Quick Tunnel e o probe do tunnel nao e usado como confound do video.
- O probe do tunnel mede RTT/falha aplicacional, nao identifica o salto interno da Cloudflare.
- Audio e sincronismo A/V nao fazem parte deste ensaio.
`;
}

async function makeOutputDir(name) {
  const base = path.join(ROOT, 'ensaio-resultados');
  const output = path.join(base, safeName(name));
  await fs.mkdir(output, { recursive: true });
  return output;
}

async function saveCalibration(output, name, calibration) {
  await Promise.all([
    fs.writeFile(path.join(output, `${name}.csv`), `${toCsv(calibration.records)}\n`),
    fs.writeFile(path.join(output, `${name}.json`), JSON.stringify(calibration, null, 2)),
  ]);
}

async function closeLaunchedBrowser(browserCdp, browser) {
  if (!browser.launched) return;
  try {
    await browserCdp.send('Browser.close', {}, 5000);
  } catch {
    browser.child?.kill();
  }
  await sleep(500);
  if (browser.profileDir) {
    const resolved = path.resolve(browser.profileDir);
    const tempRoot = path.resolve(os.tmpdir());
    if (
      resolved.startsWith(tempRoot + path.sep) &&
      path.basename(resolved).startsWith('discord-locutor-ensaio-')
    ) {
      let lastError = null;
      for (let attempt = 0; attempt < 20; attempt++) {
        try {
          await fs.rm(resolved, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
          return;
        } catch (error) {
          lastError = error;
          await sleep(250);
        }
      }
      throw lastError ?? new Error(`Nao foi possivel remover o perfil temporario ${resolved}`);
    }
  }
}

async function commandCalibrate(args) {
  const port = Number(args.cdp_port ?? 9444);
  const profileDir = await fs.mkdtemp(path.join(os.tmpdir(), 'discord-locutor-ensaio-'));
  const output = await makeOutputDir(`${timestampName()}-calibracao-medidor`);
  const browser = await ensureBrowser({ port, headless: true, profileDir });
  const browserCdp = await connectBrowser(browser);
  let pattern = null;
  try {
    pattern = await createPage(browserCdp, browser.base);
    await installPattern(pattern.cdp);
    const calibration = await calibrateVisualMeter({
      viewer: pattern.cdp,
      pattern: pattern.cdp,
      monitor: new SystemMonitor(),
      tunnel: new TunnelProbe(DEFAULT_ORIGIN, 'localhost'),
      quality: null,
      plateauMs: Number(args.plateau_ms ?? 2000),
    });
    await saveCalibration(output, 'calibracao-medidor', calibration);
    const metadata = {
      startedAt: new Date().toISOString(),
      origin: 'about:blank (controle local do codec)',
      path: 'localhost',
      transport: 'codec-visual',
      chrome: browser.version.Browser,
    };
    await fs.writeFile(path.join(output, 'metadata.json'), JSON.stringify(metadata, null, 2));
    await fs.writeFile(
      path.join(output, 'relatorio.md'),
      reportMarkdown(metadata, calibration, null, [], null),
    );
    console.log(`\n${calibration.evaluation.pass ? 'PASS' : 'FAIL'} calibracao do medidor`);
    for (const plateau of calibration.evaluation.plateaus) {
      console.log(
        `  configurado ${plateau.configuredDelayMs} ms -> mediana ${plateau.medianLagMs.toFixed(1)} ms`,
      );
    }
    console.log(`  delta final: ${calibration.evaluation.finalDelta.toFixed(1)} ms`);
    console.log(`  artefatos: ${output}`);
    if (!calibration.evaluation.pass) process.exitCode = 1;
  } finally {
    pattern?.cdp.close();
    await closeLaunchedBrowser(browserCdp, browser);
    browserCdp.close();
  }
}

async function waitForQuietHost(monitor, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  const recent = [];
  while (Date.now() < deadline) {
    const sample = await monitor.sample();
    recent.push(sample.cpuPercent);
    if (recent.length > 5) recent.shift();
    if (
      recent.length === 5 &&
      Math.max(...recent) < 70 &&
      sample.freeMemoryBytes >= CRITERIOS.memoriaLivreMinBytes
    ) {
      return { quiet: true, cpu: [...recent], freeMemoryBytes: sample.freeMemoryBytes };
    }
    await sleep(1000);
  }
  const last = await monitor.sample();
  return { quiet: false, cpu: recent, freeMemoryBytes: last.freeMemoryBytes };
}

async function commandRun(args) {
  const origin = String(args.origin ?? DEFAULT_ORIGIN).replace(/\/$/, '');
  const transport = normalizeTransport(args.transport);
  const pathLabel = normalizePath(args.path);
  const rtcPolicy = String(args.rtc_policy ?? '').toLowerCase();
  const reuseCalibrationDir = args.reuse_calibration
    ? path.resolve(ROOT, String(args.reuse_calibration))
    : null;
  impairmentBackend = String(args.impairment ?? 'cdp').toLowerCase();
  if (!['cdp', 'wsl-netem'].includes(impairmentBackend)) {
    throw new Error('Use --impairment cdp ou wsl-netem.');
  }
  if (rtcPolicy && transport !== 'webrtc') {
    throw new Error('--rtc-policy so pode ser usado com --transport webrtc.');
  }
  if (rtcPolicy && !['all', 'relay'].includes(rtcPolicy)) {
    throw new Error('Use --rtc-policy all ou relay.');
  }
  if (transport === 'webrtc' && rtcPolicy !== 'relay' && impairmentBackend === 'wsl-netem') {
    throw new Error('WebRTC direto nao atravessa o proxy WSL; use --impairment cdp.');
  }
  netemHandlesP2p = transport === 'webrtc' && rtcPolicy === 'relay';
  const official = Boolean(args.official);
  const skipQualityTrial = Boolean(args.skip_quality_trial);
  const quiesceBeforePostGate = Boolean(args.quiesce_before_post_gate);
  if (quiesceBeforePostGate && !skipQualityTrial) {
    throw new Error(
      '--quiesce-before-post-gate exige --skip-quality-trial; a histerese precisa da captura ativa.',
    );
  }
  const repetitions = Number(args.repetitions ?? 3);
  if (quiesceBeforePostGate && repetitions !== 1) {
    throw new Error(
      '--quiesce-before-post-gate exige --repetitions 1; rode replicas independentes para recriar a captura entre gates.',
    );
  }
  const variantArg = String(args.variant ?? 'all').toUpperCase();
  if (!['ALL', 'A', 'B'].includes(variantArg)) {
    throw new Error('Use --variant A, --variant B ou omita para executar ambas.');
  }
  const variants = variantArg === 'ALL' ? ['A', 'B'] : [variantArg];
  if (transport === 'webtransport' && pathLabel !== 'localhost') {
    throw new Error(
      'WebTransport fica em localhost: tunnel HTTPS/TCP nao encaminha UDP/QUIC 4443.',
    );
  }
  const capability = await fetchJson(`${origin}/api/transports`);
  if (transport === 'webtransport' && !capability.webtransport?.url) {
    throw new Error('Servidor nao anuncia WebTransport em /api/transports.');
  }
  let reusedCalibration = null;
  if (reuseCalibrationDir) {
    const [sourceMetadata, meter, impairment] = await Promise.all([
      fs.readFile(path.join(reuseCalibrationDir, 'metadata.json'), 'utf8').then(JSON.parse),
      fs
        .readFile(path.join(reuseCalibrationDir, 'calibracao-medidor.json'), 'utf8')
        .then(JSON.parse),
      fs
        .readFile(path.join(reuseCalibrationDir, 'calibracao-impairment.json'), 'utf8')
        .then(JSON.parse),
    ]);
    if (
      sourceMetadata.transport !== transport ||
      sourceMetadata.impairmentBackend !== impairmentBackend ||
      !meter.evaluation?.pass ||
      !impairment.evaluation?.pass
    ) {
      throw new Error(
        `Calibracao reutilizada nao corresponde ao ensaio: ${JSON.stringify({ sourceMetadata, meterPass: meter.evaluation?.pass, impairmentPass: impairment.evaluation?.pass })}`,
      );
    }
    reusedCalibration = { meter, impairment };
  }

  const port = Number(args.cdp_port ?? DEFAULT_CDP_PORT);
  const profileDir = await fs.mkdtemp(path.join(os.tmpdir(), 'discord-locutor-ensaio-'));
  const methodLabel =
    transport === 'webrtc' ? (rtcPolicy === 'relay' ? 'webrtc-turn' : 'webrtc-direct') : transport;
  const output = await makeOutputDir(`${timestampName()}-${methodLabel}-${pathLabel}`);
  // O gate vivo abre seus próprios listeners QUIC e exercita o binding por
  // dezenas de segundos. Rodá-lo com a captura oficial já conectada congela ou
  // envelhece a própria sessão que será medida, convertendo carga do oráculo em
  // "latência" do produto. Execute todos os gates "antes" antes mesmo de abrir
  // o Chrome do ensaio; os gates "depois" continuam após a captura ser
  // quiescida. Assim ambos validam o mesmo código sem compartilhar a janela de
  // mídia cujo resultado decidem.
  const preMediaWtGates = new Map();
  if (official && transport === 'webtransport') {
    for (const variant of variants) {
      for (let repetition = 1; repetition <= repetitions; repetition++) {
        const key = `${variant}${repetition}`;
        preMediaWtGates.set(key, await runWtGate(path.join(output, `gate-${key}-antes.log`)));
      }
    }
  }
  const runId = `DL-${Date.now().toString(36).toUpperCase()}-${process.pid.toString(36).toUpperCase()}`;
  const captureTitle = `ENSAIO RELOGIO VISUAL ${runId}`;
  const browser = await ensureBrowser({
    port,
    headless: false,
    profileDir,
    autoSelectTitle: process.platform === 'win32' ? captureTitle : null,
    secureOrigin: new URL(origin).protocol === 'http:' ? new URL(origin).origin : null,
  });
  const browserCdp = await connectBrowser(browser);
  const metadata = {
    startedAt: new Date().toISOString(),
    origin,
    path: pathLabel,
    transport,
    method: methodLabel,
    rtcPolicy: rtcPolicy || null,
    impairmentBackend,
    chrome: browser.version.Browser,
    cdpProtocol: browser.version['Protocol-Version'],
    serverCapability: capability,
    official,
    qualityTrialEnabled: !skipQualityTrial,
    quiesceBeforePostGate,
    repetitions,
    variants,
    calibrationReusedFrom: reuseCalibrationDir,
    runId,
    captureTitle,
    captureSelectionMode:
      process.platform === 'win32' ? 'playwright-automatizada-verificada' : 'manual-verificada',
    impairmentMethod:
      impairmentBackend === 'wsl-netem'
        ? 'tc netem no eth0 do proxy/TURN WSL, no caminho real dos pacotes'
        : transport === 'webrtc'
          ? 'CDP Network.emulateNetworkConditions P2P aplicado no emissor WebRTC'
          : 'CDP Network.emulateNetworkConditionsByRule aplicado no receptor',
    criteria: CRITERIOS,
  };
  await fs.writeFile(path.join(output, 'metadata.json'), JSON.stringify(metadata, null, 2));

  const pattern = await createPage(browserCdp, browser.base);
  await installPattern(pattern.cdp, captureTitle);
  const source = await createPage(browserCdp, browser.base, origin, sourceCapturePreload(runId));
  const { browserContextId: viewerContextId } = await browserCdp.send(
    'Target.createBrowserContext',
  );
  const viewer = await createPage(
    browserCdp,
    browser.base,
    origin,
    transportPreload(transport),
    viewerContextId,
  );
  await Promise.all([
    labelPage(source.cdp, `ENSAIO ORIGEM ${runId}`, 'origem'),
    labelPage(viewer.cdp, `ENSAIO ESPECTADOR ${runId}`, 'espectador'),
  ]);
  await browserCdp.send('Target.activateTarget', { targetId: source.id });
  const quality = new QualityProbe(browser.base, [pattern.id]);
  const monitor = new SystemMonitor();
  const tunnel = new TunnelProbe(origin, pathLabel);
  const rl = createInterface({ input: process.stdin, output: process.stdout });

  try {
    console.log(`\nAbri tres abas no perfil de ensaio (${browser.base}):`);
    console.log(`  1. ${captureTitle} - escolha EXATAMENTE esta aba no seletor.`);
    console.log(
      `  2. ENSAIO ORIGEM ${runId} - crie/entre na sala e transmita em 2,5 Mb/s, 30 fps.`,
    );
    console.log(
      `  3. ENSAIO ESPECTADOR ${runId} - entre na mesma sala e assista pelo ${transport}.`,
    );
    console.log(
      'Nao escolha janela/tela inteira: escolha a aba do relogio para preservar a grade de pixels.',
    );
    let ready;
    if (process.platform === 'win32') {
      console.log('Automatizando sala, identidades, picker e espectador por Playwright/UIA.');
      await runStreaming(
        process.execPath,
        [path.join(ROOT, 'scripts', 'automatizar-ensaio-aberto.mjs'), runId, browser.base],
        {
          cwd: ROOT,
          timeoutMs: 120_000,
          env: {
            ...process.env,
            ...(rtcPolicy ? { ENSAIO_RTC_POLICY: rtcPolicy } : {}),
          },
        },
      );
      ready = await waitForAutomaticReady({
        source: source.cdp,
        viewer: viewer.cdp,
        runId,
        transport,
      });
    } else {
      console.log(
        'A selecao e manual por decisao medida; titulo/runId e marcador serao verificados.',
      );
      ready = await waitForManualReady({
        rl,
        source: source.cdp,
        viewer: viewer.cdp,
        runId,
        transport,
      });
    }
    const { sourceCapture, observed } = ready;
    const pickerAudit = await source.cdp.evaluate('globalThis.__ENSAIO_PICKER_AUDIT ?? null');
    const captureSelection = evaluateCaptureSelection({
      runId,
      label: sourceCapture?.label ?? '',
      pickerSelectionName: pickerAudit?.selectedAccessibleName,
      displaySurface: sourceCapture?.settings?.displaySurface,
      markerDecoded: observed.valid,
    });
    metadata.captureSelection = {
      sourceCapture,
      pickerAudit,
      viewer: observed,
      evaluation: captureSelection,
      errors: { source: null, viewer: null },
    };
    await fs.writeFile(
      path.join(output, 'captura-verificacao.json'),
      JSON.stringify(metadata.captureSelection, null, 2),
    );
    await fs.writeFile(path.join(output, 'metadata.json'), JSON.stringify(metadata, null, 2));
    if (!captureSelection.pass) {
      throw new Error(
        `Fonte compartilhada nao confere com ${captureTitle}: ${JSON.stringify(metadata.captureSelection)}`,
      );
    }
    if (rtcPolicy === 'relay' && !/WebRTC via TURN/i.test(observed.transport ?? '')) {
      throw new Error(`--rtc-policy relay nao fechou via TURN: ${observed.transport}`);
    }
    if (
      transport === 'webrtc' &&
      rtcPolicy !== 'relay' &&
      !/WebRTC direto P2P/i.test(observed.transport ?? '')
    ) {
      throw new Error(`WebRTC direto nao fechou P2P: ${observed.transport}`);
    }
    console.log(
      `Captura confirmada por picker/runId e pixels; ${observed.transport}; lag visual inicial ${observed.visualLagMs} ms.`,
    );

    const meterCalibration = reusedCalibration
      ? { ...reusedCalibration.meter, reusedFrom: reuseCalibrationDir }
      : await calibrateVisualMeter({
          viewer: viewer.cdp,
          pattern: pattern.cdp,
          monitor,
          tunnel,
          quality,
        });
    await saveCalibration(output, 'calibracao-medidor', meterCalibration);
    console.log(
      `${meterCalibration.evaluation.pass ? 'PASS' : 'FAIL'} controle do medidor; delta final ${fmt(meterCalibration.evaluation.finalDelta)} ms.`,
    );

    let impairmentCalibration = null;
    if (meterCalibration.evaluation.pass) {
      impairmentCalibration = reusedCalibration
        ? { ...reusedCalibration.impairment, reusedFrom: reuseCalibrationDir }
        : await calibrateImpairment({
            viewer: viewer.cdp,
            source: source.cdp,
            pattern: pattern.cdp,
            monitor,
            tunnel,
            quality,
            transport,
          });
      await saveCalibration(output, 'calibracao-impairment', impairmentCalibration);
      console.log(
        `${impairmentCalibration.evaluation.pass ? 'PASS' : 'FAIL'} controle do impairment ${transport}; delta final ${fmt(impairmentCalibration.evaluation.finalDelta)} ms.`,
      );
    }

    const series = [];
    let qualityResult = null;
    if (official && meterCalibration.evaluation.pass && impairmentCalibration?.evaluation.pass) {
      const durations = { baseline: 20_000, ramp: 5000, sustain: 25_000, recovery: 30_000 };
      for (const variant of variants) {
        for (let repetition = 1; repetition <= repetitions; repetition++) {
          const qualityReady =
            transport === 'webrtc'
              ? { ready: true, snapshot: null }
              : await waitQualityCeiling(quality);
          const viewerReady = await waitViewerClean(viewer.cdp);
          if (!viewerReady.ready) {
            throw new Error(
              `Viewer nao voltou ao estado limpo antes da serie ${variant}${repetition}: ${JSON.stringify(viewerReady.snapshot)}`,
            );
          }
          const quiet = await waitForQuietHost(monitor);
          console.log(
            `\nSerie ${variant}${repetition}: host ${quiet.quiet ? 'quieto' : 'ainda ocupado; carga sera registrada'}.`,
          );
          const gateBefore =
            transport === 'webtransport' ? preMediaWtGates.get(`${variant}${repetition}`) : null;
          console.log(`gate antes: ${gateBefore?.state ?? 'n/a'}`);
          const records = await runDelaySeries({
            viewer: viewer.cdp,
            source: source.cdp,
            transport,
            variant,
            monitor,
            tunnel,
            quality,
            durations,
          });
          if (transport === 'webtransport' && quiesceBeforePostGate) {
            await source.cdp.evaluate(
              `(() => {
                for (const track of globalThis.__ENSAIO_CAPTURE_STREAM?.getTracks?.() ?? []) track.stop();
                return true;
              })()`,
            );
            await sleep(2000);
            await waitForQuietHost(monitor, 30_000);
          }
          const gateAfter =
            transport === 'webtransport'
              ? await runWtGate(path.join(output, `gate-${variant}${repetition}-depois.log`))
              : null;
          console.log(`gate depois: ${gateAfter?.state ?? 'n/a'}`);
          const evaluation = evaluateDelaySeries(records, {
            instrumentOk: meterCalibration.evaluation.pass,
            impairmentOk: impairmentCalibration.evaluation.pass,
            transport,
            path: pathLabel,
            qualityReady: qualityReady.ready,
            gateBefore: gateBefore?.state,
            gateAfter: gateAfter?.state,
          });
          const entry = {
            variant,
            repetition,
            quietAtStart: quiet,
            qualityAtStart: qualityReady,
            viewerAtStart: viewerReady,
            gateBefore: gateBefore?.state ?? null,
            gateAfter: gateAfter?.state ?? null,
            evaluation,
          };
          series.push(entry);
          await Promise.all([
            fs.writeFile(
              path.join(output, `serie-${variant}${repetition}.csv`),
              `${toCsv(records)}\n`,
            ),
            fs.writeFile(
              path.join(output, `serie-${variant}${repetition}.json`),
              JSON.stringify(entry, null, 2),
            ),
          ]);
          console.log(`${variant}${repetition}: ${evaluation.status}/${evaluation.reason}`);
        }
      }

      if (skipQualityTrial) {
        qualityResult = { status: 'NAO-RODADO', reason: 'omitido-explicitamente' };
      } else {
        const qualityGateBefore =
          transport === 'webtransport'
            ? await runWtGate(path.join(output, 'gate-qualidade-antes.log'))
            : null;
        qualityResult = await runQualityTrial({ viewer: viewer.cdp, transport, quality });
        const qualityGateAfter =
          transport === 'webtransport'
            ? await runWtGate(path.join(output, 'gate-qualidade-depois.log'))
            : null;
        if (
          transport === 'webtransport' &&
          (qualityGateBefore.state !== 'verde' || qualityGateAfter.state !== 'verde')
        ) {
          qualityResult = {
            ...qualityResult,
            measuredStatus: qualityResult.status,
            status: 'INCONCLUSIVO',
            reason: 'gate',
            gateBefore: qualityGateBefore.state,
            gateAfter: qualityGateAfter.state,
          };
        }
      }
      await fs.writeFile(
        path.join(output, 'qualidade-histerese.json'),
        JSON.stringify(qualityResult, null, 2),
      );
    }

    await fs.writeFile(path.join(output, 'series-resumo.json'), JSON.stringify(series, null, 2));
    await fs.writeFile(
      path.join(output, 'relatorio.md'),
      reportMarkdown(metadata, meterCalibration, impairmentCalibration, series, qualityResult),
    );
    console.log(`\nArtefatos: ${output}`);
    if (!official) {
      console.log(
        'Series oficiais NAO foram executadas. Reexecute com --official somente apos revisar a calibracao.',
      );
    }
    if (!meterCalibration.evaluation.pass || !impairmentCalibration?.evaluation.pass)
      process.exitCode = 1;
  } finally {
    rl.close();
    await setNetworkConditions(viewer.cdp, null).catch(() => {});
    await setNetworkConditions(source.cdp, null, { p2p: transport === 'webrtc' }).catch(() => {});
    pattern.cdp.close();
    source.cdp.close();
    viewer.cdp.close();
    await browserCdp
      .send('Target.disposeBrowserContext', { browserContextId: viewerContextId })
      .catch(() => {});
    await closeLaunchedBrowser(browserCdp, browser);
    browserCdp.close();
  }
}

function printHelp() {
  console.log(`
Ensaio de delay em rede real (Chrome/CDP)

  npm run ensaio:rede -- calibrar
  npm run ensaio:rede -- rodar --transport websocket --path localhost --origin http://localhost:3100
  npm run ensaio:rede -- rodar --transport webtransport --path localhost --origin http://localhost:3100
  npm run ensaio:rede -- rodar --transport webtransport --path localhost --origin http://localhost:3100 --official --repetitions 1 --variant B
  npm run ensaio:rede -- rodar --transport webrtc --rtc-policy all --path quick-tunnel --origin https://...trycloudflare.com
  npm run ensaio:rede -- rodar --transport webrtc --rtc-policy relay --path quick-tunnel --origin https://...trycloudflare.com

O comando rodar executa somente os dois controles positivos por padrao. Depois de
revisar seus CSVs, acrescente --official para A/B N=3 e histerese. WebTransport
publico por tunnel HTTPS/TCP e recusado porque QUIC precisa de UDP direto.
Use --variant A/B para isolar uma serie em perfil novo e --skip-quality-trial
somente quando a histerese ja foi medida por outra rodada da mesma matriz. Em
sessao isolada, --quiesce-before-post-gate encerra a captura depois da coleta e
antes do gate final para o ensaio nao disputar CPU com o proprio verificador.
`);
}

const args = parseArgs(process.argv.slice(2));
try {
  if (args.command === 'calibrar') await commandCalibrate(args);
  else if (args.command === 'rodar') await commandRun(args);
  else printHelp();
} catch (error) {
  console.error(`\nFALHOU: ${error.stack ?? error.message}`);
  process.exitCode = 1;
}
