/**
 * O painel administrativo é uma função pura: recebe o estado das salas e
 * devolve o que a tela desenha. Tudo o que se testa aqui é agregação — a
 * mesma pessoa em duas salas é uma pessoa, o mesmo servidor visto por três
 * salas é um servidor.
 */
import { describe, expect, it } from 'vitest';
import { buildAdminDashboard, mergeAdminDashboards } from './admin.js';

function trafego(recebido = 0, transmitido = 0) {
  return {
    receivedBytes: recebido,
    transmittedBytes: transmitido,
    droppedBytes: 0,
    receivedBytesPerSecond: 0,
    transmittedBytesPerSecond: 0,
    droppedBytesPerSecond: 0,
  };
}

function usuario(id, extra = {}) {
  return {
    id,
    name: `Pessoa ${id}`,
    avatar: null,
    roles: ['viewer'],
    connections: 1,
    connectedAt: 1000,
    pingMs: null,
    watching: [],
    broadcasting: false,
    mediaBytesOut: 0,
    bufferedBytes: 0,
    ...extra,
  };
}

function sala(id, extra = {}) {
  return {
    id,
    name: `Sala ${id}`,
    instance: 'canal',
    guildId: null,
    guildName: null,
    channelId: null,
    isCall: false,
    locked: false,
    createdAt: 1000,
    connections: 1,
    viewers: 1,
    broadcasters: 0,
    droppedChunks: 0,
    traffic: trafego(),
    users: [],
    streams: [],
    ...extra,
  };
}

const montar = (rooms, sockets = new Set()) =>
  buildAdminDashboard({
    roomState: { rooms, traffic: trafego(10, 20), startedAt: 500 },
    sockets,
    system: { platform: 'linux' },
    configuration: { discord: true },
  });

describe('buildAdminDashboard', () => {
  it('devolve um painel vazio quando não há sala nenhuma', () => {
    const painel = montar([]);

    expect(painel.summary).toMatchObject({ users: 0, rooms: 0, guilds: 0, streams: 0 });
    expect(painel.summary.pingAverageMs).toBeNull();
    expect(painel.summary.pingMedianMs).toBeNull();
  });

  it('repassa configuração, sistema e tráfego sem mexer', () => {
    const painel = montar([]);

    expect(painel.configuration).toEqual({ discord: true });
    expect(painel.system).toEqual({ platform: 'linux' });
    expect(painel.traffic).toEqual(trafego(10, 20));
    expect(painel.startedAt).toBe(500);
  });

  it('junta a mesma pessoa vista em duas salas', () => {
    const painel = montar([
      sala('a', { users: [usuario('alice', { connections: 2, mediaBytesOut: 100 })] }),
      sala('b', {
        users: [
          usuario('alice', {
            roles: ['broadcaster'],
            broadcasting: true,
            connectedAt: 200,
            mediaBytesOut: 50,
          }),
        ],
      }),
    ]);

    expect(painel.users).toHaveLength(1);
    expect(painel.users[0]).toMatchObject({
      id: 'alice',
      rooms: ['a', 'b'],
      connections: 3,
      // O instante mais antigo é o que vale: é quando ela chegou.
      connectedAt: 200,
      broadcasting: true,
      mediaBytesOut: 150,
    });
    expect(painel.users[0].roles).toEqual(expect.arrayContaining(['viewer', 'broadcaster']));
  });

  it('põe quem transmite no topo, e desempata pelo nome', () => {
    const painel = montar([
      sala('a', {
        users: [
          usuario('c', { name: 'Carla' }),
          usuario('a', { name: 'Ana' }),
          usuario('b', { name: 'Bruno', broadcasting: true }),
        ],
      }),
    ]);

    expect(painel.users.map((u) => u.name)).toEqual(['Bruno', 'Ana', 'Carla']);
  });

  it('tira a média dos pings de cada pessoa', () => {
    const painel = montar([
      sala('a', { users: [usuario('alice', { pingMs: 10 })] }),
      sala('b', { users: [usuario('alice', { pingMs: 30 })] }),
    ]);

    expect(painel.users[0].pingMs).toBe(20);
  });

  it('agrupa as salas por servidor do Discord', () => {
    const painel = montar([
      sala('a', {
        guildId: 'g1',
        guildName: 'Servidor',
        connections: 3,
        traffic: trafego(100, 200),
        users: [usuario('alice')],
        streams: [{ slot: 0, watchers: 2 }],
      }),
      sala('b', {
        guildId: 'g1',
        isCall: true,
        connections: 2,
        traffic: trafego(50, 0),
        users: [usuario('alice'), usuario('bob')],
      }),
      sala('c', { guildId: 'g2', guildName: 'Outro', connections: 1 }),
    ]);

    const [maior, menor] = painel.guilds;
    expect(maior).toMatchObject({ id: 'g1', name: 'Servidor', rooms: 2, calls: 1, connections: 5 });
    // Duas salas, mas duas pessoas: o Set não deixa a Alice contar duas vezes.
    expect(maior.users).toBe(2);
    expect(maior.traffic.receivedBytes).toBe(150);
    expect(menor.id).toBe('g2');
  });

  it('ignora sala sem servidor na hora de agrupar', () => {
    const painel = montar([sala('a', { users: [usuario('alice')] })]);

    expect(painel.guilds).toHaveLength(0);
    expect(painel.users[0].guilds).toEqual([]);
  });

  it('carimba em cada transmissão de onde ela veio', () => {
    const painel = montar([
      sala('a', {
        guildId: 'g1',
        guildName: 'Servidor',
        channelId: 'c1',
        streams: [{ slot: 0, watchers: 3 }],
      }),
    ]);

    expect(painel.streams[0]).toMatchObject({
      slot: 0,
      roomId: 'a',
      roomName: 'Sala a',
      guildId: 'g1',
      guildName: 'Servidor',
      channelId: 'c1',
    });
    expect(painel.summary.activeWatchers).toBe(3);
  });

  it('resume os pings das conexões abertas em média, mediana e p95', () => {
    const sockets = new Set([
      { __rttMs: 10 },
      { __rttMs: 20 },
      { __rttMs: 30 },
      { __rttMs: 40 },
      // Sem medida ainda: não entra na conta em vez de contar como zero.
      { __rttMs: null },
    ]);

    const painel = montar([sala('a')], sockets);

    expect(painel.summary.pingAverageMs).toBe(25);
    expect(painel.summary.pingMedianMs).toBe(30);
    expect(painel.summary.pingP95Ms).toBe(40);
    expect(painel.summary.connections).toBe(5);
  });

  it('soma espectadores e transmissores de todas as salas', () => {
    const painel = montar([
      sala('a', { viewers: 2, broadcasters: 1 }),
      sala('b', { viewers: 3, broadcasters: 2 }),
    ]);

    expect(painel.summary).toMatchObject({
      viewerConnections: 5,
      broadcasterConnections: 3,
      rooms: 2,
    });
  });
});

/**
 * A junção de várias máquinas num painel só.
 *
 * O que se prova aqui é o que não se enxerga olhando a tela: que a mesma
 * pessoa em duas máquinas continua sendo uma pessoa, que o mesmo servidor
 * visto de dois lugares continua sendo um servidor, e que máquina calada não
 * derruba o painel. Errar qualquer um desses faz o total mentir — e um total
 * que mente é pior que nenhum, porque ninguém desconfia dele.
 */
const parte = (index, dashboard, online = true) => ({
  index,
  origin: `https://n${index}.teste`,
  online,
  dashboard,
});

describe('mergeAdminDashboards', () => {
  it('soma salas e transmissões das máquinas, marcando de quem é cada uma', () => {
    const a = montar([sala('a1'), sala('a2')]);
    const b = montar([sala('b1')]);

    const junto = mergeAdminDashboards([parte(0, a), parte(1, b)]);

    expect(junto.summary.rooms).toBe(3);
    expect(junto.rooms.map((r) => r.id).sort()).toEqual(['a1', 'a2', 'b1']);
    expect(junto.rooms.find((r) => r.id === 'b1').node).toBe(1);
  });

  it('a mesma pessoa em duas máquinas conta como uma', () => {
    // Acontece de verdade: duas calls da mesma pessoa caem em máquinas
    // diferentes porque o sorteio é por canal.
    const a = montar([sala('a1', { users: [usuario('u1')] })]);
    const b = montar([sala('b1', { users: [usuario('u1')] })]);

    const junto = mergeAdminDashboards([parte(0, a), parte(1, b)]);

    expect(junto.summary.users).toBe(1);
    expect(junto.users).toHaveLength(1);
    // Mas as conexões dela somam: são duas de verdade.
    expect(junto.users[0].connections).toBe(2);
    expect(junto.users[0].rooms.sort()).toEqual(['a1', 'b1']);
    expect(junto.users[0].nodes).toEqual([0, 1]);
  });

  it('o mesmo servidor visto de duas máquinas é um servidor', () => {
    const guild = { guildId: 'g1', guildName: 'Servidor' };
    const a = montar([sala('a1', { ...guild, users: [usuario('u1')] })]);
    const b = montar([sala('b1', { ...guild, users: [usuario('u2')] })]);

    const junto = mergeAdminDashboards([parte(0, a), parte(1, b)]);

    expect(junto.summary.guilds).toBe(1);
    expect(junto.guilds[0].rooms).toBe(2);
    expect(junto.guilds[0].users).toBe(2);
  });

  it('não conta duas vezes quem está no mesmo servidor em duas máquinas', () => {
    const guild = { guildId: 'g1', guildName: 'Servidor' };
    const a = montar([sala('a1', { ...guild, users: [usuario('u1')] })]);
    const b = montar([sala('b1', { ...guild, users: [usuario('u1')] })]);

    const junto = mergeAdminDashboards([parte(0, a), parte(1, b)]);

    expect(junto.guilds[0].users).toBe(1);
  });

  it('soma o tráfego das máquinas', () => {
    const junto = mergeAdminDashboards([parte(0, montar([])), parte(1, montar([]))]);

    // Cada painel de teste reporta 10 recebidos e 20 transmitidos.
    expect(junto.traffic.receivedBytes).toBe(20);
    expect(junto.traffic.transmittedBytes).toBe(40);
  });

  it('máquina calada não derruba o painel, e aparece como offline', () => {
    const a = montar([sala('a1')]);

    const junto = mergeAdminDashboards([parte(0, a), parte(1, null, false)]);

    expect(junto.summary.rooms).toBe(1);
    expect(junto.nodes).toHaveLength(2);
    expect(junto.nodes[1]).toMatchObject({ index: 1, online: false, summary: null });
  });

  it('não inventa mediana nem p95 no total', () => {
    // Elas não se calculam a partir das medianas de cada máquina: seria preciso
    // a lista de pings de todas. Responder null é mais honesto que responder um
    // número que parece certo.
    const junto = mergeAdminDashboards([parte(0, montar([])), parte(1, montar([]))]);

    expect(junto.summary.pingMedianMs).toBeNull();
    expect(junto.summary.pingP95Ms).toBeNull();
  });

  it('a quebra por máquina sai em ordem, não na ordem de quem atendeu', () => {
    // A máquina que atendeu vem primeiro na entrada, porque é dela a base. Se
    // essa ordem vazasse para a saída, as linhas do painel dançariam a cada
    // atualização — exatamente o defeito que a junção veio corrigir.
    const junto = mergeAdminDashboards([
      parte(2, montar([])),
      parte(0, montar([])),
      parte(1, montar([])),
    ]);

    expect(junto.nodes.map((n) => n.index)).toEqual([0, 1, 2]);
  });

  it('mantém a configuração de quem atendeu', () => {
    const junto = mergeAdminDashboards([parte(1, montar([])), parte(0, montar([]))]);

    expect(junto.configuration).toEqual({ discord: true });
  });
});
