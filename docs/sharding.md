# Fatiar a carga entre máquinas

Como a Sala de Tela roda em mais de uma máquina sem quebrar as calls. Este
documento existe porque a tentativa óbvia — ligar o load balancer da Square Cloud
com duas instâncias — não só não funciona como quebra tudo, e o motivo não se
adivinha lendo nem o código nem o painel.

Nada disto está ligado por padrão: sem as variáveis de ambiente da última seção,
o servidor roda numa máquina só e tudo aqui é inerte.

## Por que o load balancer sozinho quebra

O servidor guarda tudo em memória. As salas vivem num `Map` do processo
(`server/rooms.js`), e o relay repassa os quadros só para os sockets **daquele**
`WebSocketServer` (`server/index.js`, na seção WebSocket).

Um load balancer distribui requisição a requisição. Então o transmissor abre o
socket na máquina A e o espectador na máquina B; a B não tem a sala, não tem o
`decoderConfig`, não tem o slot. Não é lentidão nem bug de rede: o quadro
simplesmente não tem para onde ir. A sala parece existir para quem transmite e
não existir para quem assiste.

**Sticky session não conserta.** Sticky prende *um cliente* a *uma máquina*. O
que este programa precisa é prender **uma call inteira** — o transmissor e todos
os espectadores, que são clientes diferentes, de IPs diferentes — na mesma
máquina. Nenhum balanceador genérico faz isso, porque ele não sabe ler qual sala
está dentro do corpo da requisição.

O nome disso é *sharding*: dividir por chave, não por requisição.

## O que o load balancer da Square Cloud é de fato

Vale registrar, porque o nome engana. Na Square Cloud, um load balancer é um
**domínio personalizado apontado para mais de uma aplicação**. O comando
`squarecloud app load-balancers` "lista seus domínios personalizados agrupados
pelas aplicações que os compartilham", e a referência da API descreve o
comportamento como *"traffic is balanced across the applications at the edge with
automatic failover"*.

Ou seja: revezamento na borda, com failover. Sem peso, sem health check
configurável, sem afinidade de sessão, sem roteamento por caminho — a resposta da
API não tem campo nenhum para isso. É exatamente o revezamento cego que derruba
as calls.

A boa notícia está na primeira frase: são **aplicações separadas**, cada uma com
seu próprio endereço. Essa é justamente a topologia de que o sharding precisa.
Não é preciso desmontar o que já está montado — muda só quem decide o destino:

- o domínio do balanceador continua atendendo o que **não tem estado**: o shell
  da página e o `/api/config`;
- o que **tem estado** — sala e relay de vídeo — passa a ser endereçado direto à
  aplicação dona daquela call.

## A chave: o canal, não a instância

Cada call vira uma sala com id derivado do canal de voz (`call-<channelId>`, veja
`salaDaCall` em `server/index.js`). Essa é a chave de shard.

A escolha do canal em vez da instância da Activity é deliberada. A instância
**muda a cada relançamento** da atividade no mesmo canal — o código já lida com
isso em `ensureCallRoom`, atualizando a instância da sala e mantendo o id. Se o
shard fosse pela instância, a call migraria de máquina a cada relaunch, e por
alguns segundos existiriam duas salas com o mesmo id em máquinas diferentes,
cada uma com metade das pessoas. O canal é estável, então a call fica onde está.

Como consequência, todas as salas de um mesmo canal caem na mesma máquina — e
`listRooms`, `createRoom` e `join`, que são escopados por instância, continuam
coerentes sem nenhuma consulta entre máquinas. Nenhum estado compartilhado,
nenhum Redis, nenhuma sincronização.

Fora do Discord a chave é `'web'`, uma só. O lobby do site inteiro fica numa
máquina. É uma decisão de escopo, não um esquecimento — veja os limites no fim.

## Como o cliente chega na máquina certa

Sem redirect e sem ida extra ao servidor. O cliente já sabe `sdk.channelId`
assim que o `sdk.ready()` volta, **antes** de chamar o `/api/session`. Então ele
calcula o nó localmente, com a mesma função de hash que o servidor usa, e já bate
no endereço certo de primeira.

Isso importa: o `/api/session` foi deliberadamente engordado para devolver a sala
junto da sessão e economizar uma ida de ~400ms (a nota está lá no código). Um
esquema de "pergunta onde é, depois vai lá" devolveria esse custo.

```
sdk.ready()
  │
  ├── channelId ──► nodeFor(channelId)  ──► n1
  │                 (mesma função nos dois lados)
  │
  └──► /n1/api/session ──────────────► máquina 1
       /n1/api/rooms/* ─────────────► máquina 1
       /n1/ws          ─────────────► máquina 1   ◄── relay inteiro aqui
```

Dentro do Discord isso vira um **prefixo de caminho na mesma origem**
(`/.proxy/n1/...`), porque as URL mappings do portal roteiam por prefixo. Sem
CORS, sem mexer no CSP, sem tocar na cadeia de `frame-ancestors` que já custou um
dia de depuração.

Fora do Discord vira a **origem absoluta** da aplicação
(`https://tela-discord-n1.squareweb.app`), lida do `/api/config`, que o cliente
já busca no arranque.

O servidor não confia nisso. Se chegar um pedido cuja chave não é dele, responde
`409 wrong_node` com o endereço certo, e o cliente repina e repete uma vez —
mesmo padrão do retry de identidade que já existe no `post()`. É o que segura um
cliente com bundle velho ou uma borda mal configurada, em vez de deixar a pessoa
numa sala fantasma.

## Como está feito

### `shared/shard.js`

Módulo puro, sem dependências, importado pelos dois lados — o cliente já importa
de `shared/`, e o servidor serve `/shared` estático para a página de captura.

- `shardKey({ channel, instance })` — o canal, senão a instância, senão `'web'`.
- `nodeFor(key, nodes)` — índice da máquina, por *rendezvous hash*: calcula um
  score por máquina e fica com o maior. Mudar a quantidade de máquinas move só
  1/N das chaves; com módulo simples, mudar N reembaralharia todas as calls de
  uma vez.
- Hash FNV-1a de 32 bits, síncrono. `crypto.subtle` não serve aqui: é assíncrono
  no navegador, e a escolha do nó precisa acontecer em linha reta antes do
  primeiro pedido.
- `basePathFor(i)` — `/n<i>`; `stripNode(caminho)` — o inverso, usado no servidor.

**O FNV-1a sozinho não serviu**, e isso custou um teste vermelho antes de
aparecer. Ele espalha mal os bits altos, e o rendezvous compara justamente o
valor inteiro. Com ids de canal parecidos entre si — e são: snowflakes do Discord
são quase sequenciais — a divisão saía torta e crescer de duas para três máquinas
movia **metade** das calls em vez de um terço, que é exatamente o defeito que o
rendezvous existe para evitar. A correção são cinco linhas de mistura final
(o finalizador do murmur3) no fim da função. Sem elas o esquema parece funcionar
e só cobra a conta no dia em que se acrescenta uma máquina.

### `server/index.js`

- Variáveis novas: `SHARD_INDEX` (qual máquina é esta), `SHARD_NODES` (quantas
  são) e `NODE_ORIGINS` (os endereços públicos, na mesma ordem). Sem elas, o
  sharding fica desligado e nada muda — importante para quem roda em casa ou num
  VPS único.
- O middleware que já tira o prefixo `/.proxy` passa a tirar também `/n<d>`,
  nessa ordem. Mesma coisa no `server.on('upgrade')`, que já faz esse `replace`
  para o WebSocket.
- Prefixo presente mas de outra máquina → `421 Misdirected Request`. Diz "a borda
  te trouxe ao lugar errado", que é diferente de "a sala não existe" — e essa
  distinção é o que evita um dia inteiro de depuração quando o mapping estiver
  errado.
- Um verificador de dono aplicado em `/api/session`, `/api/rooms/call`,
  `/api/rooms/create`, `/api/rooms/join`, `/api/rooms/list`,
  `/api/rooms/password` e `/api/rooms/open`: chave de outra máquina → `409` com
  o endereço certo. Ficam **de fora** as rotas sem estado, que qualquer máquina
  atende: `/api/config`, `/api/token`, `/api/ice`, `/api/health` e as de
  identidade.
- No `/api/session` a checagem vem **antes** da ida ao Discord, e o `channel_id`
  passou a ser validado antes dela: fatiar pelo valor cru mandaria a sessão para
  uma máquina e as chamadas de sala seguintes para outra, porque um id recusado
  pelo formato não entra no token e a sala vira `atividade-<id>`.
- No upgrade do WebSocket, a mesma checagem — `409` para chave de outra máquina,
  `421` para prefixo errado. O token de sala já leva o `channel` assinado dentro
  dele, então dá para recusar antes de aceitar a conexão. Sem isso o socket
  subiria numa máquina sem a sala e a pessoa ficaria olhando uma tela preta, com
  o relay funcionando perfeitamente do outro lado.
- `issueRoomTokens`: o `shareUrl` passa a apontar para a origem da máquina dona.
  Assim a página de captura já carrega do lugar certo — e `server/public/share.js`
  **não muda**, porque ele monta o endereço do socket a partir do `location.host`
  e vai acertar sozinho.
- `/api/config` passa a devolver a lista de origens e o total de máquinas, para o
  cliente fora do Discord.
- **CORS entre as máquinas do conjunto**, e só quando há mais de uma. Existe por
  causa de quem abre o site fora do Discord: ali o cliente fala com a máquina da
  sala pela origem absoluta dela, e um POST com `Content-Type: application/json`
  dispara verificação prévia. Sem isso o navegador barra o pedido antes de ele
  sair e o servidor nem fica sabendo — o sintoma é uma sala que não abre com o
  log limpo dos dois lados. A lista permitida são as próprias máquinas e o
  domínio de entrada, nunca `*`, e sem `Allow-Credentials`: o acesso à sala viaja
  em token no corpo, não em cookie. Dentro da Activity nada disso é usado, porque
  lá é tudo mesma origem.

Os tokens já atravessam máquinas sem trabalho nenhum: a assinatura é HMAC com
`SESSION_SECRET` (`server/tokens.js`), então basta as aplicações compartilharem o
mesmo segredo.

### `client/src/main.js`

- Um `S` (base do shard) ao lado do `P` que já existe para o `/.proxy`. Dentro do
  Discord vale `${P}/n<i>`; fora, a origem absoluta. Sem sharding fica igual ao
  `P`, e nenhuma URL muda de forma.
- `S` é fixado **pela resposta do servidor**, não por cálculo do cliente. Toda
  resposta que entrega tokens de sala traz também `node`, dizendo de quem é a
  sala, e o `openRoom` fixa a base a partir dele antes de qualquer conexão.
- O cálculo local continua existindo como palpite inicial, mas ele não é mais o
  que decide. Ele depende da config ter chegado, e quando ela atrasa a base fica
  no ponto de entrada, que reveza. Para o HTTP isso se conserta sozinho (`409` e
  repete); **para o WebSocket não existe essa saída** — ele só fecha, e o
  navegador nem expõe o motivo. Foi exatamente esse buraco que derrubou a
  produção; ver a seção de cicatrizes no fim.
- As chamadas de sala passam de `${P}` para `${S}`. `/api/config`, `/api/token` e
  `/api/ice` ficam no `${P}`: não têm estado, qualquer máquina responde.
- O `connect()` e o `wsUrl` do broadcaster montam o endereço com `wsBase()`, que
  tira o host da base absoluta quando ela existe e da página quando não.
- O `post()` trata o `409 wrong_node`: repina `S` e repete uma vez. É o que salva
  quem chegou com a config atrasada ou com bundle velho.

### Testes

- `shared/shard.test.js` — estabilidade da chave, distribuição entre máquinas, e
  a propriedade que separa o rendezvous do módulo: ao crescer, quem sai vai para
  a máquina nova, e ninguém pula entre as antigas.
- `server/index-shard.test.js` — arquivo à parte, como o `index-admin`, porque a
  decisão é tomada no corpo do módulo: com sharding ligado é outro servidor. Cobre
  o `409`, o `421`, o corte do `/n<d>` sozinho e combinado com `/.proxy`, a
  fronteira de caminho, o `shareUrl` e o upgrade do WebSocket. Os ids de canal são
  **procurados** pela própria função de hash, não escritos à mão: um id fixo viraria
  mentira silenciosa no dia em que o hash mudasse.

## O que configurar fora do repositório

**As três variáveis.** Nenhuma delas existe no `.env.example`; com todas
ausentes, o sharding fica desligado.

| Variável       | O que é                                              | Exemplo                                     |
| -------------- | ---------------------------------------------------- | ------------------------------------------- |
| `SHARD_NODES`  | Quantas máquinas existem. `1` (padrão) desliga tudo.  | `2`                                         |
| `SHARD_INDEX`  | Qual delas é esta. Muda em cada aplicação.            | `0`                                         |
| `NODE_ORIGINS` | As origens públicas, **na ordem dos índices**.        | `https://n0.exemplo,https://n1.exemplo`     |

O servidor **não sobe** se `NODE_ORIGINS` não tiver exatamente `SHARD_NODES`
endereços. É de propósito: sem a lista completa não há como dizer a quem chegou
na porta errada para onde ir, e o defeito só apareceria no primeiro cliente
perdido, longe da causa.

**Square Cloud.** N aplicações, cada uma com seu `SUBDOMAIN` (`tela-discord-n0`,
`tela-discord-n1`, …) e, nas variáveis de ambiente, o mesmo `SESSION_SECRET` — é
ele que faz os tokens valerem nas duas —, o mesmo `SHARD_NODES` e `NODE_ORIGINS`,
mudando só o `SHARD_INDEX`. O domínio personalizado continua apontado para todas:
ele vira a porta de entrada do que não tem estado. Vale conferir no painel o
limite de aplicações por domínio, que varia por plano.

**Portal do Discord, URL mappings.** É aqui que o roteamento por prefixo
acontece de verdade dentro da Activity, e **sem isto nada funciona**: o
balanceador da Square Cloud reveza requisições e não sabe ler caminho, então um
pedido `/n1/...` cairia na máquina 0 metade das vezes e receberia `421`. As
mappings aceitam `prefixo → host`, uma por máquina:

```
/     →  domínio do balanceador
/n0   →  tela-discord-n0.squareweb.app
/n1   →  tela-discord-n1.squareweb.app
```

**`infra/Caddyfile`.** Para quem estiver atrás de VPS em vez do balanceador, um
exemplo comentado de `handle_path /n0/*`. Vale lembrar o que o `docs/vps.md` já
diz: nesse arranjo toda a banda do relay atravessa o Caddy, e você paga a banda
duas vezes.

## O que isto não resolve

Vale ser explícito, porque é fácil esperar demais de sharding:

- **Uma call gigante continua numa máquina só.** O sharding divide *entre* calls,
  nunca *dentro* de uma. Uma call com um transmissor e cinquenta espectadores
  pesa igual depois da mudança. Quem alivia esse caso é o caminho WebRTC direto
  (`shared/rtc.js`), e ele depende de um TURN configurado: hoje `TURN_URL` é
  opcional, e sem ele todo mundo atrás de CGNAT — operadora móvel, rede
  corporativa — cai de volta no relay. Se a dor é essa, o TURN vem antes do
  sharding.
- **O lobby do site fica numa máquina só.** Fatiar o web exigiria juntar o
  `/api/rooms/list` de todas as máquinas, e o ganho não paga o custo enquanto a
  carga vier das calls.
- **Máquina que reinicia derruba as salas dela.** As salas já são efêmeras e o
  cliente reconecta sozinho; mudar a quantidade de máquinas tem o mesmo efeito,
  limitado a 1/N das calls.

## Como conferir

1. `npm test` e `npm run lint`. A suíte cobre as duas configurações: os arquivos
   antigos rodam com sharding desligado, o `index-shard` com ele ligado.
2. **Sem as variáveis de shard**, `npm run dev` e `npm run smoke`: o
   comportamento tem de ser idêntico ao de antes. Essa é a rede de proteção de
   quem roda em casa, e é o caminho que mais precisa não quebrar.
3. **Duas máquinas na mão**, em portas diferentes, com `SHARD_NODES=2`,
   `SHARD_INDEX=0` e `1`, o mesmo `SESSION_SECRET` e um `NODE_ORIGINS` apontando
   para as duas portas:
   - dois `channelId` diferentes têm de escolher máquinas diferentes;
   - bater na máquina errada tem de dar `409 wrong_node` com o endereço certo;
   - bater com prefixo de outra máquina tem de dar `421`;
   - duas abas na mesma sala, pelo `share.html`, têm de ver o vídeo passar — o
     que só acontece se as duas caíram na mesma máquina.
4. **Em produção**, com uma call real: abrir o `/admin` e olhar a tabela
   **Máquinas**. Ela mostra o total no topo e a quebra por máquina embaixo — se
   as linhas tiverem números parecidos, dividiu.

   Não tente abrir o painel no subdomínio de cada máquina: o cookie do painel é
   gravado no domínio de entrada e não é enviado para os subdomínios, então lá
   você só encontra a tela de login de novo. É por isso que quem junta é o
   servidor, e não o navegador.

## Cicatrizes

O que quebrou de verdade quando isto foi ligado em produção, para ninguém
reaprender do jeito caro.

**Um laço de reconexão sem freio derrubou a atividade inteira.** Quando o aperto
de mão do WebSocket é recusado, o cliente descartava o token e refazia o fluxo
todo *na hora*, sem espera. Esse código foi escrito para uma causa só — token
vencido —, onde falhar duas vezes seguidas era improvável. O sharding criou uma
recusa que **se repete** (a máquina errada responde `409`), e o mesmo código
virou laço apertado.

O que transformou isso em incêndio: **todo o tráfego da Activity sai pelos poucos
IPs do proxy do Discord**. Não são milhares de usuários com endereços diferentes
— para a borda, é um punhado de IPs. Alguns clientes em laço já parecem um
ataque, e a resposta foi `429` para todo mundo, inclusive para quem não estava em
laço. E o `429` realimenta: com a borda barrando, a `/api/config` passa a falhar,
mais clientes ficam sem saber a máquina, mais entram no laço.

Três lições que valem além deste projeto:

- **Todo caminho de recuperação automática precisa de freio**, mesmo o que "só
  dispara em caso raro". O que torna raro é a causa, e causas novas aparecem.
- **O navegador não expõe o status de um aperto de mão de WebSocket que falhou.**
  `401` e `409` chegam iguais. Por isso o freio é por tempo e tentativa, nunca
  por causa.
- **Deploy não alcança quem já está com a atividade aberta.** O JavaScript velho
  segue rodando na memória até a pessoa fechar e abrir. Quando o conserto é no
  cliente, a única alavanca imediata é **tirar o gatilho do lado do servidor** —
  no caso, desligar o `SHARD_NODES`, que faz o `409` deixar de existir e os
  clientes velhos pararem sozinhos.

**O que ajudou a achar:** o site fora do Discord continuou funcionando o tempo
todo. A única diferença entre os dois caminhos era que o do site espera a config
antes de agir, e o do Discord não — o que apontou direto para a base indefinida.
Quando um caminho quebra e o outro não, a diferença entre eles é o bug.

## Fontes

- [`squarecloud app load-balancers`](https://docs.squarecloud.app/pt-br/cli-reference/commands/app/load-balancers.md)
- [List Load Balancers (API)](https://docs.squarecloud.app/en/api-reference/endpoint/apps/network/load-balancers.md)
- [`squarecloud app network`](https://docs.squarecloud.app/pt-br/cli-reference/commands/app/network.md)
