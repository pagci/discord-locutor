/**
 * Gate independente do modo WS-only (Sprint 02, §9).
 *
 * Executa em uma cópia descartável para que `npm ci --omit=optional` nunca
 * toque o workspace do autor. O processo filho recebe WT explicitamente
 * desabilitado e não herda nenhum caminho do spike live.
 */
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import { dirname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const raiz = fileURLToPath(new URL('..', import.meta.url));
const temporario = await mkdtemp(join(tmpdir(), 'discord-locutor-ws-only-'));
const copia = join(temporario, 'repo');
const node = process.execPath;
const npmCli = join(dirname(node), 'node_modules', 'npm', 'bin', 'npm-cli.js');
const WT_PACKAGES = [
  '@fails-components/webtransport',
  '@fails-components/webtransport-transport-http3-quiche',
];
const ROLLUP_WINDOWS = '@rollup/rollup-win32-x64-msvc';
const ROLLUP_VERSION = '4.62.4';

const ambiente = {
  ...process.env,
  WEBTRANSPORT_ENABLED: 'false',
  PORT: '0',
};
delete ambiente.WT_LIVE_NODE_MODULES;
delete ambiente.WEBTRANSPORT_LIVE;

function ignorar(origem) {
  const rel = relative(raiz, origem);
  if (!rel || rel.startsWith(`..${sep}`)) return true;
  const primeiro = rel.split(/[\\/]/)[0];
  return !new Set(['.git', 'node_modules', 'dist', '.claydis', 'coverage']).has(primeiro);
}

function executar(comando, args, opcoes = {}) {
  return new Promise((resolve, reject) => {
    const filho = spawn(comando, args, {
      cwd: opcoes.cwd ?? copia,
      env: ambiente,
      stdio: opcoes.capture ? ['ignore', 'pipe', 'pipe'] : 'inherit',
      windowsHide: true,
    });
    let saida = '';
    if (opcoes.capture) {
      filho.stdout.on('data', (chunk) => (saida += chunk));
      filho.stderr.on('data', (chunk) => (saida += chunk));
    }
    filho.once('error', reject);
    filho.once('exit', (code, signal) => {
      if (code === 0) return resolve(saida);
      reject(
        new Error(
          `${comando} ${args.join(' ')} saiu com ${code ?? signal}${saida ? `\n${saida}` : ''}`,
        ),
      );
    });
    opcoes.onSpawn?.(filho);
  });
}

async function sha256(path) {
  return createHash('sha256')
    .update(await readFile(path))
    .digest('hex');
}

async function provarWtAusente(etapa) {
  const requireDaCopia = createRequire(join(copia, 'package.json'));
  for (const pacote of WT_PACKAGES) {
    const fisico = join(copia, 'node_modules', ...pacote.split('/'));
    const existe = await readdir(fisico).then(
      () => true,
      (error) => {
        if (error.code === 'ENOENT') return false;
        throw error;
      },
    );
    if (existe) throw new Error(`${etapa}: addon WT existe em ${fisico}`);
    try {
      requireDaCopia.resolve(pacote);
      throw new Error(`${etapa}: addon WT ainda resolve: ${pacote}`);
    } catch (error) {
      if (error.code !== 'MODULE_NOT_FOUND') throw error;
    }
  }
  console.log(`PASS ${etapa}: addons WT ausentes e nao resolvem`);
}

async function inventarioPacotes(nodeModules) {
  const pacotes = new Set();
  async function visitar(modulesDir) {
    const entradas = await readdir(modulesDir, { withFileTypes: true }).catch((error) => {
      if (error.code === 'ENOENT') return [];
      throw error;
    });
    for (const entrada of entradas) {
      if (!entrada.isDirectory() || entrada.name === '.bin') continue;
      if (entrada.name.startsWith('@')) {
        const escopo = join(modulesDir, entrada.name);
        const filhos = await readdir(escopo, { withFileTypes: true });
        for (const filho of filhos) {
          if (filho.isDirectory()) await registrar(join(escopo, filho.name));
        }
      } else {
        await registrar(join(modulesDir, entrada.name));
      }
    }
  }
  async function registrar(packageDir) {
    const manifest = JSON.parse(await readFile(join(packageDir, 'package.json'), 'utf8'));
    pacotes.add(`${manifest.name}@${manifest.version}`);
    await visitar(join(packageDir, 'node_modules'));
  }
  await visitar(nodeModules);
  return pacotes;
}

let servidor;
try {
  await cp(raiz, copia, { recursive: true, filter: ignorar });
  const claydisCopiado = await readdir(join(copia, '.claydis')).then(
    () => true,
    (error) => {
      if (error.code === 'ENOENT') return false;
      throw error;
    },
  );
  if (claydisCopiado) throw new Error('.claydis entrou na copia WS-only');
  console.log('PASS copia WS-only exclui .claydis');
  await executar(node, [npmCli, 'ci', '--omit=optional']);
  await provarWtAusente('apos npm ci --omit=optional');

  const packageJson = join(copia, 'package.json');
  const packageLock = join(copia, 'package-lock.json');
  const lock = JSON.parse(await readFile(packageLock, 'utf8'));
  const lockRollup = lock.packages?.[`node_modules/${ROLLUP_WINDOWS}`]?.version;
  if (lockRollup !== ROLLUP_VERSION) {
    throw new Error(`Rollup platform fora do lock esperado: ${lockRollup ?? 'ausente'}`);
  }
  const manifestsAntes = await Promise.all([sha256(packageJson), sha256(packageLock)]);
  const pacotesAntes = await inventarioPacotes(join(copia, 'node_modules'));
  const instalacaoRollup = join(copia, '.rollup-platform-test-only');
  await mkdir(instalacaoRollup, { recursive: true });
  await writeFile(join(instalacaoRollup, 'package.json'), `${JSON.stringify({ private: true })}\n`);
  await executar(
    node,
    [
      npmCli,
      'install',
      '--no-save',
      '--ignore-scripts',
      '--package-lock=false',
      `${ROLLUP_WINDOWS}@${ROLLUP_VERSION}`,
    ],
    { cwd: instalacaoRollup },
  );
  const inventarioRollup = await inventarioPacotes(join(instalacaoRollup, 'node_modules'));
  if (inventarioRollup.size !== 1 || !inventarioRollup.has(`${ROLLUP_WINDOWS}@${ROLLUP_VERSION}`)) {
    throw new Error(`staging Rollup trouxe pacotes extras: ${[...inventarioRollup].join(',')}`);
  }
  await cp(
    join(instalacaoRollup, 'node_modules', ...ROLLUP_WINDOWS.split('/')),
    join(copia, 'node_modules', ...ROLLUP_WINDOWS.split('/')),
    { recursive: true },
  );
  await rm(instalacaoRollup, { recursive: true, force: true });
  const manifestsDepois = await Promise.all([sha256(packageJson), sha256(packageLock)]);
  if (manifestsDepois.join(':') !== manifestsAntes.join(':')) {
    throw new Error('instalacao test-only alterou package.json ou package-lock.json');
  }
  const pacotesDepois = await inventarioPacotes(join(copia, 'node_modules'));
  const adicionados = [...pacotesDepois].filter((pacote) => !pacotesAntes.has(pacote));
  const removidos = [...pacotesAntes].filter((pacote) => !pacotesDepois.has(pacote));
  if (
    adicionados.length !== 1 ||
    adicionados[0] !== `${ROLLUP_WINDOWS}@${ROLLUP_VERSION}` ||
    removidos.length
  ) {
    throw new Error(
      `reificacao fora da excecao; adicionados=${adicionados.join(',')} removidos=${removidos.join(',')}`,
    );
  }
  await provarWtAusente('apos excecao Rollup test-only');
  console.log(`PASS manifests imutaveis: ${manifestsDepois.join(' ')}`);
  await executar(node, [npmCli, 'test', '--', '--reporter=dot']);
  await executar(node, [npmCli, 'run', 'build']);

  const porta = 32100 + Math.floor(Math.random() * 1000);
  ambiente.PORT = String(porta);
  ambiente.PUBLIC_ORIGIN = `http://127.0.0.1:${porta}`;
  ambiente.SMOKE_BASE = ambiente.PUBLIC_ORIGIN;
  ambiente.SMOKE_WS = `ws://127.0.0.1:${porta}`;

  let resolverReady;
  let rejeitarReady;
  const ready = new Promise((resolve, reject) => {
    resolverReady = resolve;
    rejeitarReady = reject;
  });
  servidor = spawn(node, ['server/index.js'], {
    cwd: copia,
    env: ambiente,
    stdio: ['ignore', 'pipe', 'pipe'],
    windowsHide: true,
  });
  const observar = (chunk) => {
    const texto = String(chunk);
    process.stdout.write(texto);
    if (texto.includes('Sala de Tela no ar')) resolverReady();
  };
  servidor.stdout.on('data', observar);
  servidor.stderr.on('data', observar);
  servidor.once('exit', (code) =>
    rejeitarReady(new Error(`servidor saiu antes do smoke: ${code}`)),
  );
  await Promise.race([
    ready,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error('servidor não ficou ready')), 10_000),
    ),
  ]);
  await executar(node, [npmCli, 'run', 'smoke']);
  console.log('PASS verify-ws-only: instalação sem opcionais, testes, build, start e smoke');
} finally {
  if (servidor && servidor.exitCode === null) {
    servidor.kill('SIGTERM');
    await new Promise((resolve) => servidor.once('exit', resolve));
  }
  await rm(temporario, { recursive: true, force: true });
}
