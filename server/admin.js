function average(values) {
  const valid = values.filter(Number.isFinite);
  return valid.length ? valid.reduce((sum, value) => sum + value, 0) / valid.length : null;
}

function percentile(values, fraction) {
  const valid = values.filter(Number.isFinite).sort((a, b) => a - b);
  if (!valid.length) return null;
  return valid[Math.min(valid.length - 1, Math.floor(valid.length * fraction))];
}

function addTraffic(target, source) {
  for (const key of [
    'receivedBytes',
    'transmittedBytes',
    'droppedBytes',
    'receivedBytesPerSecond',
    'transmittedBytesPerSecond',
    'droppedBytesPerSecond',
  ]) {
    target[key] = (target[key] ?? 0) + (source?.[key] ?? 0);
  }
}

function emptyTraffic() {
  return {
    receivedBytes: 0,
    transmittedBytes: 0,
    droppedBytes: 0,
    receivedBytesPerSecond: 0,
    transmittedBytesPerSecond: 0,
    droppedBytesPerSecond: 0,
  };
}

export function buildAdminDashboard({ roomState, sockets, system, configuration }) {
  const rooms = roomState.rooms;
  const users = new Map();
  const guilds = new Map();
  const streams = [];

  for (const room of rooms) {
    let guild = null;
    if (room.guildId) {
      guild = guilds.get(room.guildId);
      if (!guild) {
        guild = {
          id: room.guildId,
          name: room.guildName ?? null,
          rooms: 0,
          calls: 0,
          users: new Set(),
          connections: 0,
          streams: 0,
          traffic: emptyTraffic(),
        };
        guilds.set(room.guildId, guild);
      }
      if (room.guildName) guild.name = room.guildName;
      guild.rooms++;
      if (room.isCall) guild.calls++;
      guild.connections += room.connections;
      guild.streams += room.streams.length;
      addTraffic(guild.traffic, room.traffic);
    }

    for (const user of room.users) {
      let aggregate = users.get(user.id);
      if (!aggregate) {
        aggregate = {
          id: user.id,
          name: user.name,
          avatar: user.avatar,
          guilds: new Set(),
          rooms: new Set(),
          roles: new Set(),
          connections: 0,
          connectedAt: user.connectedAt,
          pings: [],
          broadcasting: false,
          mediaBytesOut: 0,
          bufferedBytes: 0,
        };
        users.set(user.id, aggregate);
      }

      aggregate.name = user.name || aggregate.name;
      aggregate.avatar = user.avatar ?? aggregate.avatar;
      aggregate.rooms.add(room.id);
      if (room.guildId) aggregate.guilds.add(room.guildId);
      for (const role of user.roles) aggregate.roles.add(role);
      aggregate.connections += user.connections;
      aggregate.connectedAt = Math.min(aggregate.connectedAt, user.connectedAt);
      if (Number.isFinite(user.pingMs)) aggregate.pings.push(user.pingMs);
      aggregate.broadcasting ||= user.broadcasting;
      aggregate.mediaBytesOut += user.mediaBytesOut;
      aggregate.bufferedBytes += user.bufferedBytes;
      guild?.users.add(user.id);
    }

    for (const stream of room.streams) {
      streams.push({
        ...stream,
        roomId: room.id,
        roomName: room.name,
        guildId: room.guildId,
        guildName: room.guildName,
        channelId: room.channelId,
      });
    }
  }

  const userList = [...users.values()]
    .map((user) => ({
      id: user.id,
      name: user.name,
      avatar: user.avatar,
      guilds: [...user.guilds],
      rooms: [...user.rooms],
      roles: [...user.roles],
      connections: user.connections,
      connectedAt: user.connectedAt,
      pingMs: average(user.pings),
      broadcasting: user.broadcasting,
      mediaBytesOut: user.mediaBytesOut,
      bufferedBytes: user.bufferedBytes,
    }))
    .sort(
      (a, b) => Number(b.broadcasting) - Number(a.broadcasting) || a.name.localeCompare(b.name),
    );

  const guildList = [...guilds.values()]
    .map((guild) => ({
      ...guild,
      users: guild.users.size,
    }))
    .sort((a, b) => b.connections - a.connections);

  const pings = [...sockets].map((socket) => socket.__rttMs).filter(Number.isFinite);
  const activeWatchers = streams.reduce((sum, stream) => sum + stream.watchers, 0);

  return {
    generatedAt: Date.now(),
    startedAt: roomState.startedAt,
    configuration,
    summary: {
      users: userList.length,
      connections: sockets.size,
      viewerConnections: rooms.reduce((sum, room) => sum + room.viewers, 0),
      broadcasterConnections: rooms.reduce((sum, room) => sum + room.broadcasters, 0),
      activeWatchers,
      streams: streams.length,
      rooms: rooms.length,
      guilds: guildList.length,
      pingAverageMs: average(pings),
      pingMedianMs: percentile(pings, 0.5),
      pingP95Ms: percentile(pings, 0.95),
    },
    traffic: roomState.traffic,
    system,
    guilds: guildList,
    rooms,
    streams,
    users: userList,
  };
}

/**
 * Junta os painéis de várias máquinas num só.
 *
 * Sem isto, o painel atrás de um balanceador mostra a realidade de uma máquina
 * por vez — e como cada atualização sorteia outra, os números piscam entre
 * mundos diferentes. Com cinco máquinas não dá nem para ler.
 *
 * Quem junta é o servidor, não o navegador. O cookie do painel é gravado no
 * domínio de entrada e não é enviado para os subdomínios das máquinas, então
 * perguntar de dentro do navegador exigiria um login por máquina.
 *
 * O resultado vai no topo, no mesmo formato de sempre: o painel existente passa
 * a mostrar o total sem precisar saber que existe mais de uma máquina. A quebra
 * por máquina vai em `nodes`, ao lado.
 *
 * @param {Array<{index: number, origin: string, online: boolean, dashboard: object|null}>} partes
 */
export function mergeAdminDashboards(partes) {
  const vivas = partes.filter((p) => p.online && p.dashboard);
  // A máquina que atendeu é a primeira da lista e sempre está viva: é dela o
  // que não se junta (a configuração, e o `system`, que é do container dela).
  const base = vivas[0]?.dashboard ?? partes[0]?.dashboard ?? null;
  if (!base) return null;

  const rooms = [];
  const streams = [];
  const users = new Map();
  const traffic = emptyTraffic();

  for (const { index, dashboard } of vivas) {
    // A máquina vai junto de cada sala e transmissão: sem isso não há como
    // saber onde uma call está morando, que é metade da pergunta.
    for (const room of dashboard.rooms ?? []) rooms.push({ ...room, node: index });
    for (const stream of dashboard.streams ?? []) streams.push({ ...stream, node: index });
    addTraffic(traffic, dashboard.traffic);

    for (const user of dashboard.users ?? []) {
      const atual = users.get(user.id);
      if (!atual) {
        users.set(user.id, { ...user, nodes: [index] });
        continue;
      }

      // A mesma pessoa em duas calls que caíram em máquinas diferentes. Somar
      // as duas linhas contaria uma pessoa como duas — é justamente o erro que
      // torna o total inútil.
      atual.nodes.push(index);
      atual.rooms = [...new Set([...atual.rooms, ...user.rooms])];
      atual.guilds = [...new Set([...atual.guilds, ...user.guilds])];
      atual.roles = [...new Set([...atual.roles, ...user.roles])];
      atual.connectedAt = Math.min(atual.connectedAt, user.connectedAt);
      atual.broadcasting ||= user.broadcasting;
      atual.mediaBytesOut += user.mediaBytesOut;
      atual.bufferedBytes += user.bufferedBytes;
      atual.pingMs = mediaPonderada(
        [atual.pingMs, user.pingMs],
        [atual.connections, user.connections],
      );
      atual.connections += user.connections;
    }
  }

  const userList = [...users.values()].sort(
    (a, b) => Number(b.broadcasting) - Number(a.broadcasting) || a.name.localeCompare(b.name),
  );

  // Servidores contados a partir da lista de pessoas já unida, e não somando os
  // contadores de cada máquina: canais diferentes do mesmo servidor caem em
  // máquinas diferentes, e somar contaria de novo quem está nos dois.
  const guilds = juntarGuilds(vivas, userList);

  const soma = (campo) =>
    vivas.reduce((total, { dashboard }) => total + (dashboard.summary?.[campo] ?? 0), 0);

  return {
    ...base,
    generatedAt: Date.now(),
    summary: {
      users: userList.length,
      connections: soma('connections'),
      viewerConnections: soma('viewerConnections'),
      broadcasterConnections: soma('broadcasterConnections'),
      activeWatchers: soma('activeWatchers'),
      streams: streams.length,
      rooms: rooms.length,
      guilds: guilds.length,
      pingAverageMs: mediaPonderada(
        vivas.map((p) => p.dashboard.summary?.pingAverageMs),
        vivas.map((p) => p.dashboard.summary?.connections ?? 0),
      ),
      // Mediana e p95 não se juntam a partir de medianas e p95: seria preciso a
      // lista de pings de todas as máquinas. Preferimos não responder a
      // responder um número que parece certo e não é — cada máquina tem os
      // dela, exatos, em `nodes`.
      pingMedianMs: null,
      pingP95Ms: null,
    },
    traffic,
    guilds,
    rooms,
    streams,
    users: userList,
    // Ordenada por índice aqui, e não por quem chamou: a lista de entrada vem
    // com a máquina que atendeu na frente (é dela a base), mas o painel precisa
    // mostrar sempre na mesma ordem, senão as linhas dançam a cada atualização
    // — que é o defeito que este trabalho todo veio corrigir.
    nodes: [...partes]
      .sort((a, b) => a.index - b.index)
      .map(({ index, origin, online, dashboard }) => ({
        index,
        origin,
        online,
        startedAt: dashboard?.startedAt ?? null,
        summary: dashboard?.summary ?? null,
        traffic: dashboard?.traffic ?? null,
        system: dashboard?.system ?? null,
      })),
  };
}

/** Média de valores com pesos diferentes; ignora o que não é número. */
function mediaPonderada(valores, pesos) {
  let soma = 0;
  let total = 0;
  for (let i = 0; i < valores.length; i++) {
    const valor = valores[i];
    if (!Number.isFinite(valor)) continue;
    // Peso zero ainda conta como uma amostra: sem isso, uma máquina sem
    // conexões apagaria o ping dela do total em vez de pesar pouco.
    const peso = Number.isFinite(pesos[i]) && pesos[i] > 0 ? pesos[i] : 1;
    soma += valor * peso;
    total += peso;
  }
  return total ? soma / total : null;
}

/** Servidores unidos por id, com as pessoas contadas sem repetição. */
function juntarGuilds(vivas, userList) {
  const guilds = new Map();

  for (const { dashboard } of vivas) {
    for (const guild of dashboard.guilds ?? []) {
      const atual = guilds.get(guild.id);
      if (!atual) {
        guilds.set(guild.id, { ...guild, traffic: { ...guild.traffic } });
        continue;
      }
      atual.name = guild.name ?? atual.name;
      atual.rooms += guild.rooms;
      atual.calls += guild.calls;
      atual.connections += guild.connections;
      atual.streams += guild.streams;
      addTraffic(atual.traffic, guild.traffic);
    }
  }

  for (const guild of guilds.values()) guild.users = 0;
  for (const user of userList) {
    for (const id of user.guilds) {
      const guild = guilds.get(id);
      if (guild) guild.users++;
    }
  }

  return [...guilds.values()].sort((a, b) => b.connections - a.connections);
}
