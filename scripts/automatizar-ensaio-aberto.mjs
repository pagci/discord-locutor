import { execFileSync } from 'node:child_process';
import { chromium } from 'playwright-core';
import { hasStableGateTail, isResolvedGateSample } from './ensaio-delay-lib.mjs';

const runId = process.argv[2];
const endpoint = process.argv[3] ?? 'http://127.0.0.1:9333';

if (!runId) throw new Error('Uso: node scripts/automatizar-ensaio-aberto.mjs <runId> [cdp]');

const browser = await chromium.connectOverCDP(endpoint);
const pages = browser.contexts().flatMap((context) => context.pages());

async function pageWithTitle(expected) {
  for (const page of pages) {
    if ((await page.title()) === expected) return page;
  }
  throw new Error(`Aba nao encontrada: ${expected}`);
}

const source = await pageWithTitle(`ENSAIO ORIGEM ${runId}`);
const viewer = await pageWithTitle(`ENSAIO ESPECTADOR ${runId}`);
const roomName = `Ensaio ${runId}`;
const captureTitle = `ENSAIO RELOGIO VISUAL ${runId}`;
const udpImpairment = process.env.ENSAIO_UDP_IMPAIRMENT === '1';
const rtcPolicy = process.env.ENSAIO_RTC_POLICY || (udpImpairment ? 'relay' : '');
const expectedTransport = await viewer.evaluate(() => globalThis.__ENSAIO_TRANSPORT ?? null);

if (!['websocket', 'webtransport', 'webrtc'].includes(expectedTransport)) {
  throw new Error(`Transporte esperado ausente ou invalido: ${expectedTransport}`);
}

if (rtcPolicy) {
  if (!['all', 'relay'].includes(rtcPolicy)) throw new Error(`rtcPolicy invalida: ${rtcPolicy}`);
  for (const [page, title] of [
    [source, `ENSAIO ORIGEM ${runId}`],
    [viewer, `ENSAIO ESPECTADOR ${runId}`],
  ]) {
    const cdp = await page.context().newCDPSession(page);
    await cdp.send('Network.enable');
    await cdp.send('Network.setCacheDisabled', { cacheDisabled: true });
    await page.evaluate((policy) => {
      const url = new URL(globalThis.location.href);
      url.searchParams.set('rtcPolicy', policy);
      globalThis.history.replaceState(null, '', url);
    }, rtcPolicy);
    await page.reload({ waitUntil: 'domcontentloaded' });
    await page.evaluate((expectedTitle) => {
      globalThis.document.title = expectedTitle;
    }, title);
  }
  console.log(`[auto] bundle sem cache; rtcPolicy=${rtcPolicy} em origem e espectador`);
}

async function freshGuest(page, name) {
  return page.evaluate(async (guestName) => {
    const response = await fetch('/api/session-guest', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: guestName }),
    });
    if (!response.ok) throw new Error(`session-guest ${response.status}`);
    const guest = await response.json();
    localStorage.setItem('identity', guest.identity);
    for (const key of Object.keys(localStorage)) {
      if (key.startsWith('sala:')) localStorage.removeItem(key);
    }
    return guest.identity;
  }, name);
}

const [sourceIdentity, viewerIdentity] = await Promise.all([
  freshGuest(source, 'Origem do ensaio'),
  freshGuest(viewer, 'Espectador do ensaio'),
]);
if (viewerIdentity === sourceIdentity) throw new Error('Identidades de origem/espectador iguais');

await Promise.all([
  source.reload({ waitUntil: 'domcontentloaded' }),
  viewer.reload({ waitUntil: 'domcontentloaded' }),
]);
await Promise.all([
  source.evaluate((title) => {
    globalThis.document.title = title;
  }, `ENSAIO ORIGEM ${runId}`),
  viewer.evaluate((title) => {
    globalThis.document.title = title;
  }, `ENSAIO ESPECTADOR ${runId}`),
]);

console.log(`[auto] criando sala "${roomName}"`);
await source.locator('#newRoom').waitFor({ state: 'visible', timeout: 20_000 });
await source.locator('#newRoom').click();
await source.locator('#createName').fill(roomName);
await source.locator('#createGo').click();
await source.locator('#share').waitFor({ state: 'visible', timeout: 20_000 });

await viewer.locator('#newRoom').waitFor({ state: 'visible', timeout: 20_000 });
const roomCard = viewer.locator('.room-card', { hasText: roomName });
await roomCard.waitFor({ state: 'visible', timeout: 20_000 });
await roomCard.dispatchEvent('click');
await viewer.locator('#leaveRoom').waitFor({ state: 'visible', timeout: 20_000 });
console.log('[auto] espectador independente entrou');

await source.bringToFront();
await source.locator('#share').focus();
await source.keyboard.press('Enter');
try {
  await source.waitForFunction(
    (expectedRunId) =>
      globalThis.__ENSAIO_CAPTURE_PROBE?.runId === expectedRunId &&
      ['pending', 'resolved'].includes(globalThis.__ENSAIO_CAPTURE_PROBE?.status),
    runId,
    { timeout: 30_000 },
  );
} catch (error) {
  const state = await source.evaluate(() => ({
    probe: globalThis.__ENSAIO_CAPTURE_PROBE ?? null,
    href: globalThis.location.href,
    shareDisabled: globalThis.document.querySelector('#share')?.disabled,
    shareText: globalThis.document.querySelector('#share')?.textContent?.trim(),
    toast: globalThis.document.querySelector('#toast')?.textContent?.trim(),
  }));
  throw new Error(`Captura nao iniciou apos clique confiavel: ${JSON.stringify(state)}`, {
    cause: error,
  });
}

const chooseClock = String.raw`
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName UIAutomationClient
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class EnsaioMouse {
  [DllImport("user32.dll")] public static extern bool SetCursorPos(int X, int Y);
  [DllImport("user32.dll")] public static extern void mouse_event(uint f, uint x, uint y, uint d, UIntPtr e);
}
'@
$chrome = Get-Process chrome |
  Where-Object { $_.MainWindowTitle -like 'ENSAIO ORIGEM ${runId}*' } |
  Select-Object -First 1
if (-not $chrome) { throw 'Chrome da origem nao encontrado' }
$shell = New-Object -ComObject WScript.Shell
if (-not $shell.AppActivate($chrome.Id)) { throw 'Chrome da origem nao recebeu foco' }
Start-Sleep -Milliseconds 300
$root = [System.Windows.Automation.AutomationElement]::FromHandle($chrome.MainWindowHandle)
$all = $root.FindAll([System.Windows.Automation.TreeScope]::Descendants, [System.Windows.Automation.Condition]::TrueCondition)
$item = $null
for ($i = 0; $i -lt $all.Count; $i++) {
  $candidate = $all.Item($i)
  if ($candidate.Current.Name -eq '${captureTitle}' -and
      $candidate.Current.ControlType -eq [System.Windows.Automation.ControlType]::DataItem) {
    $item = $candidate
    break
  }
}
if (-not $item) { throw 'Aba exata do relogio nao encontrada' }
$rect = $item.Current.BoundingRectangle
[EnsaioMouse]::SetCursorPos([int]($rect.X + $rect.Width / 2), [int]($rect.Y + $rect.Height / 2)) | Out-Null
Start-Sleep -Milliseconds 150
[EnsaioMouse]::mouse_event(2, 0, 0, 0, [UIntPtr]::Zero)
[EnsaioMouse]::mouse_event(4, 0, 0, 0, [UIntPtr]::Zero)
Start-Sleep -Milliseconds 300
$selection = [System.Windows.Automation.SelectionItemPattern]$item.GetCurrentPattern([System.Windows.Automation.SelectionItemPattern]::Pattern)
if (-not $selection.Current.IsSelected) { throw 'Aba do relogio nao ficou selecionada' }
$all = $root.FindAll([System.Windows.Automation.TreeScope]::Descendants, [System.Windows.Automation.Condition]::TrueCondition)
$button = $null
for ($i = 0; $i -lt $all.Count; $i++) {
  $candidate = $all.Item($i)
  if ($candidate.Current.Name -eq 'Compartilhar' -and
      $candidate.Current.ControlType -eq [System.Windows.Automation.ControlType]::Button) {
    $button = $candidate
    break
  }
}
if (-not $button -or -not $button.Current.IsEnabled) { throw 'Compartilhar indisponivel' }
$invoke = [System.Windows.Automation.InvokePattern]$button.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern)
$invoke.Invoke()
`;

let selectionMethod = 'chromium-auto-select-title';
const autoSelected = await source
  .waitForFunction(
    (expectedRunId) =>
      globalThis.__ENSAIO_CAPTURE_PROBE?.runId === expectedRunId &&
      globalThis.__ENSAIO_CAPTURE_PROBE?.status === 'resolved',
    runId,
    { timeout: 3000 },
  )
  .then(() => true)
  .catch(() => false);

if (!autoSelected) {
  selectionMethod = 'windows-uia';
  execFileSync('powershell.exe', ['-NoProfile', '-Command', chooseClock], { stdio: 'inherit' });
}
await source.evaluate(
  (audit) => {
    globalThis.__ENSAIO_PICKER_AUDIT = audit;
  },
  { runId, selectedAccessibleName: captureTitle, selectedAt: Date.now(), method: selectionMethod },
);
await source.waitForFunction(
  (expectedRunId) => {
    const probe = globalThis.__ENSAIO_CAPTURE_PROBE;
    return (
      probe?.runId === expectedRunId &&
      probe.status === 'resolved' &&
      probe.settings?.displaySurface === 'browser'
    );
  },
  runId,
  { timeout: 30_000 },
);
console.log(`[auto] picker confirmou "${captureTitle}"`);

const watchButton = viewer.locator('.watch-prompt button').first();
await watchButton.waitFor({ state: 'visible', timeout: 30_000 });
await viewer.bringToFront();
await watchButton.click();
try {
  await viewer.waitForFunction(
    (expected) => {
      const method = globalThis.document.querySelector('#viaBadge')?.textContent ?? '';
      if (expected === 'webrtc') {
        return (
          /WebRTC/i.test(method) &&
          [...globalThis.document.querySelectorAll('video')].some(
            (video) => video.readyState >= 2 && video.videoWidth > 0,
          )
        );
      }
      const methodReady =
        expected === 'webtransport' ? /WebTransport/i.test(method) : /WebSocket/i.test(method);
      const canvasReady = [...globalThis.document.querySelectorAll('canvas')].some(
        (canvas) => canvas.width > 0 && canvas.height > 0,
      );
      return methodReady && canvasReady;
    },
    expectedTransport,
    { timeout: 30_000 },
  );
} catch (error) {
  const diagnostics = {};
  for (const [name, page] of [
    ['source', source],
    ['viewer', viewer],
  ]) {
    diagnostics[name] = await page.evaluate(async () => {
      const peers = [];
      for (const diagnostic of globalThis.__ENSAIO_RTC_DIAGNOSTICS ?? []) {
        const selected = [];
        try {
          for (const stat of (await diagnostic.pc.getStats()).values()) {
            if (
              stat.type === 'candidate-pair' ||
              stat.type === 'local-candidate' ||
              stat.type === 'remote-candidate' ||
              stat.type === 'transport'
            )
              selected.push({ ...stat });
          }
        } catch {
          // O peer pode fechar entre getStats e a coleta diagnóstica; os demais
          // campos ainda são úteis para explicar a tentativa.
        }
        peers.push({
          configuration: diagnostic.pc.getConfiguration?.(),
          connectionState: diagnostic.pc.connectionState,
          iceConnectionState: diagnostic.pc.iceConnectionState,
          iceGatheringState: diagnostic.pc.iceGatheringState,
          signalingState: diagnostic.pc.signalingState,
          events: diagnostic.events,
          stats: selected,
        });
      }
      return {
        url: globalThis.location.href,
        method: globalThis.document.querySelector('#viaBadge')?.textContent?.trim() ?? '',
        peers,
      };
    });
  }
  console.error(`RTC_DIAGNOSTICS=${JSON.stringify(diagnostics)}`);
  throw error;
}
await viewer.locator('#viaBadge').waitFor({ state: 'visible', timeout: 15_000 });
console.log(`[auto] ${await viewer.locator('#viaBadge').textContent()}`);

await viewer.waitForFunction(
  () =>
    ['pass', 'fail'].includes(globalThis.document.querySelector('#qualityBadge')?.dataset.state),
  null,
  { timeout: 20_000 },
);

async function captureQualitySample() {
  return viewer.evaluate(() => {
    const optionalNumber = (value) =>
      value === undefined || value === null || value === '' ? null : Number(value);
    const videos = [...globalThis.document.querySelectorAll('video')];
    const video = videos.find((candidate) => candidate.readyState >= 2 && candidate.videoWidth > 0);
    const canvas = [...globalThis.document.querySelectorAll('canvas')]
      .filter((candidate) => candidate.width > 0 && candidate.height > 0)
      .sort((a, b) => b.width * b.height - a.width * a.height)[0];
    const playback = video?.getVideoPlaybackQuality?.();
    const qualityBadge = globalThis.document.querySelector('#qualityBadge');
    const panelResolution = globalThis.document
      .querySelector('#pRes')
      ?.textContent?.trim()
      .replace('×', 'x');
    const resolution = /^\d+x\d+$/.test(panelResolution ?? '')
      ? panelResolution
      : video
        ? `${video.videoWidth}x${video.videoHeight}`
        : canvas
          ? `${canvas.width}x${canvas.height}`
          : null;
    return {
      capturedAt: Date.now(),
      method: globalThis.document.querySelector('#viaBadge')?.textContent?.trim() ?? '',
      gate: qualityBadge?.dataset.state ?? '',
      quality: qualityBadge?.textContent?.trim() ?? '',
      bitrateBps: optionalNumber(qualityBadge?.dataset.bitrateBps),
      fps: optionalNumber(qualityBadge?.dataset.fps),
      packetLossPct: optionalNumber(qualityBadge?.dataset.packetLossPct),
      jitterMs: optionalNumber(qualityBadge?.dataset.jitterMs),
      droppedFramesPct: optionalNumber(qualityBadge?.dataset.droppedFramesPct),
      resolution,
      renderKind: video ? 'video' : canvas ? 'canvas' : null,
      readyState: video?.readyState ?? null,
      totalVideoFrames: playback?.totalVideoFrames ?? null,
      droppedVideoFrames: playback?.droppedVideoFrames ?? null,
    };
  });
}

async function collectFixed(count) {
  const samples = [];
  for (let index = 0; index < count; index++) {
    if (index) await viewer.waitForTimeout(1000);
    samples.push(await captureQualitySample());
  }
  return samples;
}

async function collectResolved(
  count,
  { maximum = count * 3, requireStablePass = false, consecutivePasses = 5 } = {},
) {
  const observed = [];
  const resolved = [];
  while (
    (resolved.length < count ||
      (requireStablePass && !hasStableGateTail(resolved, { consecutive: consecutivePasses }))) &&
    observed.length < maximum
  ) {
    if (observed.length) await viewer.waitForTimeout(1000);
    const sample = await captureQualitySample();
    observed.push(sample);
    if (isResolvedGateSample(sample)) resolved.push(sample);
  }
  return { observed, resolved };
}

async function collectUntilStablePass({ minimum = 30, maximum = 60, consecutive = 5 } = {}) {
  const samples = [];
  while (samples.length < maximum) {
    if (samples.length) await viewer.waitForTimeout(1000);
    samples.push(await captureQualitySample());
    if (samples.length >= minimum && hasStableGateTail(samples, { consecutive })) {
      break;
    }
  }
  return samples;
}

if (udpImpairment) {
  const wsl = (command) =>
    execFileSync('wsl.exe', ['-d', 'eLxr', '-u', 'root', '--', 'sh', '-lc', command], {
      stdio: 'inherit',
    });
  const clearImpairment = () => wsl('tc qdisc del dev eth0 root 2>/dev/null || true');
  const applyImpairment = () =>
    wsl(
      'tc qdisc replace dev eth0 root netem delay 120ms 40ms distribution normal loss 15% rate 450kbit',
    );

  const method = await viewer.locator('#viaBadge').textContent();
  if (!/WebRTC via TURN.*UDP/i.test(method ?? '')) {
    throw new Error(`Fluxo nao passou pelo TURN/UDP controlado: ${method}`);
  }

  let clean;
  let degraded = [];
  let recovered = [];
  try {
    clearImpairment();
    clean = await collectFixed(8);
    if (clean.some((sample) => sample.gate !== 'pass'))
      throw new Error(`Controle limpo nao permaneceu PASS: ${JSON.stringify(clean)}`);

    applyImpairment();
    let failSamples = 0;
    for (let index = 0; index < 25; index++) {
      if (index) await viewer.waitForTimeout(1000);
      const sample = await captureQualitySample();
      degraded.push(sample);
      failSamples = sample.gate === 'fail' ? failSamples + 1 : 0;
      if (failSamples >= 4) break;
    }
    if (!degraded.some((sample) => sample.gate === 'fail'))
      throw new Error(`Impairment UDP nao derrubou o gate: ${JSON.stringify(degraded)}`);

    clearImpairment();
    let consecutivePasses = 0;
    for (let index = 0; index < 30; index++) {
      if (index) await viewer.waitForTimeout(1000);
      const sample = await captureQualitySample();
      recovered.push(sample);
      consecutivePasses = sample.gate === 'pass' ? consecutivePasses + 1 : 0;
      if (consecutivePasses >= 5) break;
    }
    if (consecutivePasses < 5)
      throw new Error(`Gate nao recuperou para PASS: ${JSON.stringify(recovered)}`);
  } finally {
    clearImpairment();
  }

  console.log(
    `UDP_IMPAIRMENT_EVIDENCE=${JSON.stringify({ runId, netem: { delayMs: 120, jitterMs: 40, lossPct: 15, rateKbps: 450 }, clean, degraded, recovered })}`,
  );
} else {
  // O encoder e o decoder precisam aquecer depois que a captura entra. Essa
  // janela nao faz parte do controle oficial; primeiro exigimos estabilidade
  // e so entao medimos 30 amostras limpas.
  const qualityWarmup = await collectUntilStablePass({ minimum: 5, maximum: 30, consecutive: 5 });
  if (!hasStableGateTail(qualityWarmup)) {
    throw new Error(`Quality gate nao aqueceu para PASS: ${JSON.stringify(qualityWarmup)}`);
  }
  const { observed: qualityObserved, resolved: qualityEvidence } = await collectResolved(30, {
    maximum: 60,
    requireStablePass: true,
  });
  if (qualityEvidence.length < 30) {
    throw new Error(`Quality gate nao resolveu 30 amostras: ${JSON.stringify(qualityObserved)}`);
  }
  const stabilizedPass = hasStableGateTail(qualityEvidence);
  // Uma falha transitória é evidência útil da histerese, não motivo para
  // descartar uma rodada que voltou a cinco PASS consecutivos. Mantemos todas
  // as amostras no relatório e só autorizamos a medição após recuperação
  // estável; assim PASS -> FALHOU -> PASS continua observável, sem verde forçado.
  if (!stabilizedPass) {
    throw new Error(`Quality gate nao estabilizou em PASS: ${JSON.stringify(qualityEvidence)}`);
  }
  if (qualityEvidence.some((sample) => !sample.resolution)) {
    throw new Error(`Resolucao de chegada ausente: ${JSON.stringify(qualityEvidence)}`);
  }
  console.log(`QUALITY_WARMUP=${JSON.stringify(qualityWarmup)}`);
  console.log(
    `QUALITY_MEASURING=${JSON.stringify(qualityObserved.filter((sample) => !isResolvedGateSample(sample)))}`,
  );
  console.log(`QUALITY_EVIDENCE=${JSON.stringify(qualityEvidence)}`);
}
process.exit(0);
