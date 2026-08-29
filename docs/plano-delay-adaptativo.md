# Plano: eliminar o delay acumulativo do relay

Este documento é anterior ao código. Ele existe para registrar o diagnóstico, a
âncora externa verificada e o desenho de cada mudança, na mesma tradição do
[como-funciona.md](como-funciona.md).

## O problema

Relatado: o delay da transmissão **cresce continuamente** ao longo do tempo,
sem se estabilizar. O diagnóstico abaixo foi lido do código, não deduzido.

## Diagnóstico — as quatro raízes

### 1. Backpressure por BYTES, não por tempo

`rooms.js:34` define `MAX_BUFFERED_BYTES = 2 * 1024 * 1024`. O descarte para um
espectador só começa quando o socket dele acumula 2 MB de quadros. A 2,5 Mb/s
isso são ~6 segundos de mídia; a 8 Mb/s ainda é ~2 segundos. A fila cresce até
esse teto antes de qualquer freio atuar — o delay acumulado que se vê é,
literalmente, essa fila.

### 2. Zlib sobre mídia já comprimida

`index.js:1223` cria o `WebSocketServer` sem desligar `perMessageDeflate`, que é
ligado por padrão na biblioteca `ws`. Todo quadro acima de 1 KB é comprimido com
zlib — em H.264/Opus, dados já entropicamente comprimidos: nenhum byte economizado,
CPU gasta e latência adicionada quadro a quadro.

### 3. Correção de drift lenta demais no player

`player.js:54-57`: `AJUSTE_MS = 2000`, `PASSO_MAX_MS = 15`. O player corrige a
referência de exibição no máximo 15 ms a cada 2 s — 7,5 ms/s. Qualquer drift
maior entre chegada e exibição acumula para sempre dentro da fila, somando-se à
fila de rede da raiz 1.

### 4. TCP não descarta quadro atrasado

Estrutural ao transporte WebSocket, e já documentado neste repo como o motivo da
tentativa de WebRTC por espectador. As raízes 1–3 são amplificadas por ela: o
que TCP enfileira, ninguém joga fora antes do tempo.

## Âncora externa verificada

Duas fontes primárias do próprio Discord, lidas integralmente:

- **"How It All Goes Live: An Overview of Discord's Streaming Technology"**
  (Josh Stratton, EM de Client Audio/Video, março 2024, discord.com/blog).
- **"How Discord Handles Two and Half Million Concurrent Voice Users using
  WebRTC"** (Jozsef Vass, setembro 2018, discord.com/blog).

O que elas dizem e como este plano usa cada ponto:

| Técnica do Discord | Este repo | Este plano |
|---|---|---|
| SFU roteia só para quem assiste | já existe (`watch` é opt-in) | intacto |
| Uma codificação, limitada pela conexão mais lenta | já existe (encode único) | intacto; é também a limitação assumida em 1.4 |
| "Go Live não pode acumular segundos de buffer" — sob aperto o encoder descarta quadros e o cliente ajusta qualidade e latência | descarte existe na ORIGEM (`encodeFrame`, fila >2); no ESPECTADOR só após 2 MB | raízes 1 e 3 corrigem o lado do espectador; 1.4 leva o ajuste de qualidade ao relay |
| RTCP: SFU coleta relatórios dos receptores e avisa o emissor da banda disponível | não existe no relay (bitrate fixo da engrenagem); existe no caminho WebRTC direto (libwebrtc) | 1.4 é a versão relay desta técnica |
| Codec negociado por capacidade, hardware primeiro | já existe (`pickConfig`) | intacto |

Estado da arte consultado para 1.3: a estimativa de jitter de inter-chegada do
RFC 3550 (`J = J + (|D| - J)/16`), base dos jitter buffers de todo stack RTP.

## O desenho

### 1.1 Backpressure temporal (`rooms.js`)

O teto por espectador passa de bytes fixos para **tempo de mídia na fila**:

```
orcamento(entry) = max(MIN_ORCAMENTO_BYTES,
                       bpsEntrada(entry) * MAX_FILA_SEGUNDOS)
```

- `MAX_FILA_SEGUNDOS = 0.3` — a fila máxima que um espectador pode carregar.
- `MIN_ORCAMENTO_BYTES = 96 * 1024` — cobre keyframe completo em stream de
  bitrate baixo e o primeiro segundo de vida da stream, quando o contador ainda
  não tem amostra.
- `bpsEntrada` sai de onde já é medido: `trafficSnapshot(entry.traffic, 5)
  .receivedBytesPerSecond`. Nenhum contador novo por socket.
- Proporções atuais preservadas: keyframe pode até `2 × orcamento`; delta e
  áudio acima de `orcamento` são descartados com as mesmas ações de hoje (delta:
  desprepara + pede keyframe; keyframe estourado: pede keyframe; áudio: descarte
  simples). `droppedChunks` e o registro de tráfego não mudam.

A troca é apenas a unidade da pergunta "a fila desta pessoa passou do limite?" —
de "quantos megabytes" para "quantos milissegundos" — que é a unidade que o
delay percebe.

### 1.2 Desligar a compressão (`index.js`)

```js
new WebSocketServer({ noServer: true, maxPayload: 4 * 1024 * 1024,
                      perMessageDeflate: false })
```

JSON de controle segue sem compressão como sempre esteve na prática (mensagens
curtas); mídia deixa de pagar zlib por quadro.

### 1.3 Jitter buffer com alvo adaptativo (`player.js`)

O buffer de exibição deixa de ser uma constante e passa a acompanhar a
irregularidade medida da entrega.

- A cada quadro recebido em `push`, medir o jitter de inter-chegada do RFC 3550:
  `D = (chegada_i - chegada_{i-1}) - (ts_i - ts_{i-1})`, e
  `J += (|D| - J) / 16`.
- Alvo do buffer: `alvo = clamp(BASE + 2 * J, PISO, TETO)`, com `BASE = 40 ms`,
  `PISO = 40 ms`, `TETO = 250 ms`. Rede lisa → o delay paga só 40 ms; rede em
  rajada → o próprio buffer cresce sozinho para absorver.
- A correção periódica (hoje a cada 2 s, passo 15 ms) passa a janela de 1 s e
  conduz a âncora até o alvo, passo máximo 20 ms — com alvo móvel, a lentidão
  da correção deixa de ser gargalo.
- Dreno: quadro que chega depois da própria hora reancora (como hoje); fila que
  passa de `alvo + 150 ms` é esvaziada até o quadro mais novo e reancorada —
  atraso acumulado não se recupera sozinho, exatamente como o áudio já assume
  com o corte dos 320 ms.
- `FILA_MAX` continua como teto duro de emergência.
- Reancoragem por origem nova (timestamp andando para trás, como já existe) zera
  também o `J`.

Modos de falha e por que não assustam: estimativa instável é suavizada pelo
próprio divisor 16 do EWMA; clamp impede alvo absurdo; a reancoragem cobre o
caso em que o alvo persegue uma rede que piorou de verdade.

### 1.4 Qualidade adaptativa por feedback (técnica RTCP, versão relay)

O servidor vê o que nenhum cliente consegue ver sozinho: todos os espectadores
ao mesmo tempo. Ele decide; o transmissor executa.

**Detecção** (`rooms.js`, junto do sweeper de 4 s):

- cada descarte feito em `pushChunk` já sabe para quem foi — conta no próprio
  socket do espectador, numa janela que o sweeper zera a cada volta;
- espectador assistindo pelo relay (não pelo WebRTC) com 2 ou mais descartes na
  janela está degradando;
- uma stream em degradação = pelo menos um espectador dela degradando.

**Decisão** (por stream, estado no `entry`):

- stream degradando e passaram 2 s desde o último ajuste → `quality-down` ao
  transmissor, `entry.degraus++`;
- nenhum espectador degradando por 2 janelas seguidas e `degraus > 0` e passaram
  10 s desde o último ajuste → `quality-up`, `entry.degraus--`.
- Assimetria deliberada: descer é rápido e barato (2 s), subir é devagar (10 s +
  prova repetida de saúde). É histerese contra o laço de oscilação down/up.
- Transmissão nova ou `start` reinicia degraus e carimbos, como `config` hoje.

**Execução** (`shared/broadcaster.js`):

- `quality-down`: bitrate efetivo × 0,75, piso 300 kbps; só quando o bitrate já
  está no piso é que o fps cai (30 → 24 → 18 → 15, piso 15). Aplica com
  `setQuality`, que já reconfigura o encoder ao vivo.
- `quality-up`: o inverso (fps volta primeiro até o valor do usuário, depois o
  bitrate), sem nunca passar do que a pessoa escolheu na engrenagem — a escolha
  dela é teto, o ajuste automático acontece embaixo dele.
- Com `chunks: false` (todos os espectadores em conexão direta) o laço inteiro
  fica suspenso: não há relay sendo consumido, não há o que degradar.
- A aba de captura mostra no status quando a qualidade está reduzida
  automaticamente — ajuste invisível vira mistério depois.

**Limitação assumida, igual à do Discord**: o encode é um só, então degradar
para socorrer um espectador degrada para todos. O blog do Discord diz a mesma
coisa: o streamer não transmite mais do que a conexão mais lenta suporta. A
saída estrutural (uma qualidade por espectador) exige encode por espectador —
um SFU de verdade, fora do escopo deste projeto.

## O que NÃO muda

- Protocolo binário de pacotes, tokens, salas, opt-in de assistir, keyframe sob
  demanda, o caminho WebRTC por espectador e todos os limites de segurança.
- O formato das mensagens novas: `quality-down` / `quality-up` são mensagens de
  controle JSON servidor→transmissor, irmãs de `need-keyframe` e `chunks`; a
  seção de protocolo do `como-funciona.md` ganha as duas.

## Modos de falha do conjunto

1. **Oscilação de qualidade** (desce, melhora, sobe, degrada, desce).
   Mitigado pela histerese 2 s/10 s + prova de 2 janelas para subir.
2. **Espectador em rede ruim rebaixa a sala toda.** Assumido e documentado — é
   o mesmo trade-off do Discord com encode único. O alívio estrutural (WebRTC
   direto do transmissor para esse espectador) já existe: ele sai do relay e o
   loop o ignora.
3. **Estimativa de bitrate de entrada errada** no primeiro segundo (contador
   frio). Mitigado pelo piso absoluto em bytes do orçamento.
4. **Correção do player briga com a reancoragem.** As duas não disputam a mesma
   grandeza: a correção move a âncora em passos pequenos; a reancoragem acontece
   só quando o erro já passou do limite — uma é regime permanente, a outra é
   exceção, e a exceção zera o estado da primeira.

## Verificação

- **Unitários** (vitest, suíte existente):
  - `rooms.test.js`: orçamento temporal derruba o drop de 2 MB para ~300 ms de
    mídia; drops por espectador alimentam a detecção; cooldowns e histerese do
    1.4; `quality-up` respeita os degraus.
  - `player.test.js`: o alvo do buffer segue o jitter medido (rede lisa → ~40 ms,
    rajada → cresce); dreno acima de alvo+150 ms; relógio simulado que o arquivo
    já tem.
  - `broadcaster.test.js`: down/up com piso e teto, fps só depois do piso de
    bitrate, engrenagem do usuário como teto duro.
- **E2E**: `npm run smoke` existente (cobre relay, keyframe, opt-in) deve
  continuar verde sem ajuste — nenhuma dessas mudanças toca o protocolo.
- **A/B medido**: duas janelas locais, transmissão sob throttling do DevTools
  (~1,5 Mb/s) no espectador. Antes: `getLag()` crescendo sem teto enquanto a
  fila sobe até 2 MB. Depois: lag estabilizado em ~300–600 ms e `quality-down`
  visível no status da aba. O experimento de controle é a mesma suíte e o mesmo
  cenário sem as mudanças.

## Fora deste plano (fase 2, separada)

Transport factory (`shared/transport.js`) com WebTransport/QUIC quando
disponível e WebSocket como piso universal, negociação por conexão, validação de
passagem por Cloudflare Tunnel. Desenhado à parte, depois deste plano verificado,
porque o relay estabilizado aqui é o piso que a fábrica precisa preservar.
