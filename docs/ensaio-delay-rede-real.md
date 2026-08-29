# Ensaio de delay em rede real

Este ensaio responde com serie temporal e criterios numericos a uma pergunta
especifica: depois de a rede apertar e voltar ao normal, a imagem retorna ao
piso ou continua no passado?

Ele mede separadamente WebSocket relay, WebTransport/QUIC relay e WebRTC P2P.
Cada rodada e marcada como `localhost`, `quick-tunnel` ou `named-tunnel`, guarda
CSV bruto e classifica o resultado como `PASS`, `FAIL` ou `INCONCLUSIVO`.

## Leia antes: chegada nao e playout

`player.getLag()` nao mede o frame desenhado. Em `client/src/player.js:141`, ele
e atualizado quando o pacote chega, antes de decode e antes da fila adaptativa;
`client/src/main.js:1307` apenas mostra esse numero.

Por isso o ensaio usa duas reguas:

- **lag de chegada:** `player.getLag()`, amostrado pela UI existente;
- **lag visual:** timestamp codificado em blocos de pixels no conteudo
  compartilhado e decodificado do canvas/video efetivamente exibido;
- **espera de playout estimada:** `lag visual - lag de chegada`.

Essa diferenca fica no alto de todo relatorio gerado. Tratar `getLag()` como
playout produziria um resultado numerico preciso sobre a coisa errada.

## O que e degradado de verdade

O caminho usado e real: direto em localhost ou passando pelo tunnel publico.
A degradacao controlada, porem, e **emulada no stack do Chrome via CDP**. Esta
maquina nao tem clumsy/WinDivert, e a politica QoS nativa do Windows limita
somente throughput e exige elevacao. O ensaio nao chama CDP de `tc netem` nem de
degradacao fisica do roteador.

O Chrome 151 instalado oferece `Network.emulateNetworkConditionsByRule`. A
regra global se aplica tambem a P2P e permite latencia/throughput; para WebRTC,
tambem perda, fila e reordenacao. A reversao remove a regra inteira.

Antes de aceitar qualquer ausencia de acumulo, ha dois controles positivos:

1. o relogio visual carrega atraso conhecido `0 -> 500 -> 1000 -> 1500 ms`;
2. cada transporte recebe CDP `0 -> 400 -> 800 -> 1200 ms`.

O primeiro exige saltos medidos de pelo menos 350 ms e delta final de 1200 ms.
O segundo exige delta final de 900 ms. Falhar um controle torna a rodada
inconclusiva; nunca vira boa noticia.

## Pre-requisitos nesta maquina

- Node 24 e dependencias instaladas;
- Chrome desktop;
- servidor em `http://localhost:3100`;
- `PUBLIC_ORIGIN=http://localhost:3100` para localhost;
- para QUIC local, listener UDP em `127.0.0.1:4443` e capability publicada em
  `GET /api/transports`.

A porta 3001 nao serve nesta maquina: ela cai na faixa TCP excluida pelo Windows
`2979-3078` (Hyper-V/WSL). O contrato local usa 3100.

Comece pelo controle autonomo do medidor:

```powershell
npm run ensaio:rede -- calibrar
```

Ele abre um Chrome headless descartavel, mede os quatro plateaus, grava o CSV e
fecha apenas o browser que criou. Nenhuma serie oficial e executada.

## Rodar um transporte

Primeiro execute somente as calibracoes live, sem `--official`:

```powershell
# Relay WebSocket local
npm run ensaio:rede -- rodar --transport websocket --path localhost --origin http://localhost:3100

# Relay WebTransport/QUIC local
npm run ensaio:rede -- rodar --transport webtransport --path localhost --origin http://localhost:3100

# WebRTC direto, sinalizado pelo caminho local
npm run ensaio:rede -- rodar --transport webrtc --path localhost --origin http://localhost:3100

# WebSocket ou WebRTC via Quick Tunnel
npm run ensaio:rede -- rodar --transport websocket --path quick-tunnel --origin https://SEU-ENDERECO.trycloudflare.com
```

O comando abre um perfil dedicado do Chrome e tres abas:

1. `ENSAIO RELOGIO VISUAL <runId>`;
2. origem, para criar/entrar na sala e transmitir;
3. espectador, preparada para o transporte pedido.

Na origem, selecione 2,5 Mb/s e 30 fps. No seletor de compartilhamento escolha
**a aba cujo titulo completo e impresso no terminal**, nao a janela nem a tela
inteira. O sufixo `runId` torna a fonte explicita e unica. Quando o espectador
mostrar o relogio, volte ao terminal e pressione Enter.

Depois do Enter, o harness nao confia apenas no clique: exige que o label da
track contenha o `runId`, que `displaySurface` seja `browser` e que o marcador
de pixels seja decodificado no viewer. Os tres precisam passar. Um clique na
fonte errada para antes das calibracoes e grava `captura-verificacao.json`.

### Por que a selecao continua manual

Esta e uma decisao medida no Chrome 151, nao uma automacao esquecida:

1. `--use-fake-ui-for-media-stream` junto dos seletores desktop+tab suprimiu o
   picker, mas entregou `screen:0:0`, `displaySurface=monitor`; titulo e pixel
   nao conferiram.
2. O primeiro teste isolado dos dois bracos foi invalidado: o driver aceitou o
   `readyState=complete` do `about:blank` antes da navegacao. Ele produziu zero
   observacoes sobre as flags. O driver passou a exigir **origem esperada e
   `readyState=complete`**, e tem controle positivo para essa precondicao.
3. Com o condutor validado e somente o seletor de aba, `video:true` ainda
   escolheu o monitor. A constraint `{displaySurface:'browser'}` escolheu a
   categoria correta (`web-contents-media-stream`), mas a aba errada: o label
   nao continha o `runId` e o pixel central nao era o marcador.

Logo, a constraint dirige a categoria, mas o switch por titulo nao dirigiu a
fonte nesta build. Captura automatica que erra em silencio e pior que o custo
visivel de um clique supervisionado; as flags nao fazem parte do ensaio.

O modo sem `--official` para depois dos controles positivos. Revise
`calibracao-medidor.csv`, `calibracao-impairment.csv` e `relatorio.md` antes de
prosseguir. O Chrome visivel fica aberto para inspecao; feche-o manualmente.

So entao rode as series oficiais:

```powershell
npm run ensaio:rede -- rodar --transport websocket --path localhost --origin http://localhost:3100 --official
```

Por padrao sao tres repeticoes A e tres B. A repete todas as fases com regra
neutra; B usa baseline 20 s, rampa 5 s, sustentacao 25 s, restaura e recupera
por 30 s. A condicao comum chega a 250 ms e 600 kb/s. WebRTC ganha uma serie
adicionalmente sujeita a 5% de perda, fila de 20 pacotes e reordenacao.

## QUIC: gate e limite do tunnel

WebTransport e medido somente em localhost. O listener precisa de UDP direto em
4443; Quick Tunnel encaminha HTTPS/TCP e nao oferece esse caminho QUIC. Essa
limitacao e parte do resultado: ela e a razao operacional para o relay
WebSocket continuar existindo.

Antes e depois de **cada** serie QUIC, o harness roda `npm run test:webtransport`
e preserva o log. O estado e:

- `gate=verde`: exit 0 e nenhuma ocorrencia de `[flake]`;
- `gate=vermelho`: qualquer exit diferente de zero ou `[flake]`.

Gate vermelho domina o veredicto da serie: `INCONCLUSIVO/gate`. O numero de
delay continua no CSV, mas nao vira evidencia do player. O harness nunca edita
os oraculos locked.

## Tunnel e carga sao variaveis medidas

Quick Tunnel nao tem garantia de uptime. Em toda serie publica, o harness faz
`GET /api/transports` com cache-bust a 1 Hz pelo mesmo origin e registra RTT,
falha e erro. As series local e publica nunca sao misturadas.

Em paralelo, cada amostra guarda:

- CPU total derivada dos contadores de `os.cpus()`;
- memoria livre;
- contagem de processos `node`, `codex`, `claude`, `chrome` e `cloudflared`.

No WebRTC com duas abas locais, o tunnel carrega a sinalizacao, nao a midia que
ja ficou P2P. Por isso uma falha do probe do Quick Tunnel nao reclassifica uma
falha de video WebRTC como `INCONCLUSIVO/tunel`; ela continua registrada apenas
como contexto. Para P2P WAN real seria necessario um segundo dispositivo/rede.

Antes de uma serie oficial o harness espera ate 30 s por cinco amostras abaixo
de 70% de CPU. Durante a serie, CPU >= 90% ou memoria livre < 1 GiB por 2 s,
quando coincide com uma falha numerica, produz `INCONCLUSIVO/carga`.

No tunnel, falhas acima de 2% ou p95 mais de 500 ms acima do baseline, quando
coincidem com falha, produzem `INCONCLUSIVO/tunel`. Isso nao transforma falha
em PASS; apenas impede atribuir ao player o ruido do host/caminho.

## Criterios fixados antes de medir

| Propriedade          | Criterio                                                           |
| -------------------- | ------------------------------------------------------------------ |
| amostras validas     | >= 90%                                                             |
| pico                 | maior mediana movel de 1 s <= 2000 ms; nenhuma amostra >= 2500 ms  |
| sustentacao          | p95 <= baseline p95 + 1000 ms                                      |
| nao acumula          | slope Theil-Sen <= 10 ms/s e ultimos 5 s - primeiros 5 s <= 200 ms |
| recuperacao          | janela de 3 s <= baseline p95 + 100 ms em ate 12 s                 |
| permanece recuperado | p95 posterior <= baseline p95 + 200 ms                             |
| jitter evidenciado   | mediana degradada sobe >= 20 ms                                    |
| espera adaptativa    | sobe >= 20 ms e fica <= 300 ms                                     |
| repeticao            | 3/3 por A e B; mediana/IQR, nunca apenas media                     |

O pico de 2000 ms respeita a excecao explicita de keyframe
`TETO_KEYFRAME_SEGUNDOS = 2.0`. Os 12 s de recuperacao cobrem o pior ajuste de
210 ms a 20 ms/s do player, mais margem.

O ensaio curto de qualidade retira o impairment 1 s depois do primeiro down.
Ele exige down em 12 s, nenhum up antes de 10 s, up ate 18 s apos restore,
nenhum novo down e nenhum valor acima do teto manual 2,5 Mb/s/30 fps. Em WebRTC
direto esse item e N/A: sem viewer relay-only o relay suspende o laco.

## Saidas

Tudo fica em `ensaio-resultados/<data>-<transporte>-<caminho>/`:

- `metadata.json`: origin, caminho, transporte, Chrome, capability e criterios;
- `captura-verificacao.json`: label/runId, `displaySurface`, marcador e veredicto da fonte manual;
- `calibracao-*.csv/json`: controles positivos;
- `serie-A1.csv` ... `serie-B3.csv`: serie temporal bruta, fases marcadas;
- `serie-*.json`: checks, mediana/IQR, slope, recuperacao e confounds;
- `gate-*.log`: gates QUIC antes/depois;
- `qualidade-histerese.json`: down/up e teto;
- `relatorio.md`: resumo legivel e limites declarados.

## Limites que o relatorio nao esconde

- Nao ha contador exposto de frames velhos descartados pelo WebCodecs.
  `takeFrameCount()` conta desenhados. O descarte e provado indiretamente por
  lag limitado, continuidade e recuperacao; nao se inventa um contador.
- Duas abas na mesma maquina nao constituem P2P WAN. Este harness automatizado
  mede um unico host; a extensao para segundo dispositivo exige CDP remoto e
  calibracao de relogio que ainda nao foram implementados.
- Em duas maquinas, o timestamp visual precisa de estimativa de offset de
  relogio. Na mesma maquina ele e exato.
- O probe do tunnel nao identifica o salto interno da Cloudflare.
- Audio e sincronismo A/V estao fora deste ensaio.

Se a grade nao decodificar, confirme que o conteudo compartilhado e a aba do
relogio e que ela ocupa o frame inteiro. Para outro perfil CDP, acrescente, por
exemplo, `--cdp-port 9340`.
