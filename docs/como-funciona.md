# Como funciona (para quem mexe no código)

Este arquivo existe só para explicar as decisões que não se adivinham lendo o
código. Para instalar e usar, veja o [README](../README.md).

## Por que a tela é capturada numa aba separada

Duas restrições do Discord definiram o desenho inteiro:

1. **A atividade roda num iframe de outro domínio.** Nesse contexto o navegador
   nega `getDisplayMedia()` — a função que pede a tela — a menos que o Discord
   marque o iframe com `allow="display-capture"`, o que ele não faz.
2. **WebRTC não existe em atividades.** A documentação do Discord diz que só
   WebSocket é suportado. Sem P2P, sem SFU.

Então a captura acontece **fora** do sandbox, numa aba normal do navegador, e os
quadros vão por WebSocket para o servidor, que os repassa para quem assiste:

```
QUEM MOSTRA                        SERVIDOR              QUEM ASSISTE
aba normal do navegador                                  atividade (iframe)
  getDisplayMedia  ✅                                          │
  VideoEncoder                                                 │
  └──── WebSocket binário ────►  repassa sem                   │
                                 abrir o quadro ───────────────►
                                                          VideoDecoder → canvas
```

Quem assiste nunca sai do Discord. Só quem mostra passa por uma aba.

Se um dia o Discord conceder `display-capture`, o botão **"Testar captura no
iframe"** (no painel de detalhes) passa a funcionar — e aí a aba externa pode
sumir. A atividade já tenta capturar internamente antes de cair para a aba.

## Por que WebCodecs e não MediaRecorder

A primeira versão usava `MediaRecorder` + Media Source Extensions e ficava em
~3 segundos de atraso. O formato de container impõe um piso: o pedaço só sai
depois de fechado, e o player precisa acumular buffer para não engasgar.

WebCodecs elimina os dois. Cada quadro é codificado, enviado e desenhado
individualmente, sem container. E, ao contrário de `display-capture`, WebCodecs
não é bloqueado dentro do iframe.

## Relay oportunista: WebTransport com WebSocket universal

Viewer, aba de captura e transmissor não escolhem o transporte por conta própria.
Todos pedem um socket lógico à mesma factory. Para cada conexão ela consulta a
capability do nó dono da sala sem enviar o token, tenta WebTransport por até
1,5 segundo quando um listener HTTP/3 realmente está pronto e, antes de `OPEN`,
cai uma única vez para o WebSocket original. Capability ausente, addon opcional
ausente ou listener perdido nunca desliga o WS.

No WebTransport, JSON de controle usa um stream bidirecional enquadrado. Keyframes
usam streams unidirecionais confiáveis; deltas e áudio usam datagramas fragmentados,
com FEC XOR temporária quando a telemetria nativa confirma perda. A barreira de
controle por destinatário impede um chunk de ultrapassar seu `config`, enquanto
lanes separadas por slot e classe evitam que um áudio lento bloqueie outra tela.
Filas, assemblies e bytes retidos têm tetos e deadlines. Se um delta some, a lane
espera só 250 ms: reaproveita um keyframe confiável que já tenha chegado ou pede
outro ao transmissor, repetindo o pedido com cadência limitada até recuperar.

Isso troca apenas o relay. A tentativa WebRTC direta descrita abaixo continua
igual: quando o primeiro quadro aparece no `<video>`, aquele viewer sai do relay,
seja ele WT ou WS. O painel de diagnóstico mostra o transporte que de fato venceu
o handshake e um motivo sanitizado quando uma tentativa WT caiu para WS; nunca
deduz “QUIC ativo” apenas porque a API existe no navegador.

## Keyframe sob demanda

Quem chega no meio de uma transmissão não consegue decodificar nada até receber
um quadro completo. Em vez de guardar um antigo, o servidor **pede um novo** ao
transmissor quando alguém começa a assistir — a tela aparece em ~1 quadro.

O servidor também barra quadros incompletos para quem ainda não recebeu um
completo: alimentar um decodificador frio com eles só produz erro.

## Assistir é opt-in

O servidor não manda os quadros de uma tela para ninguém que não tenha pedido
explicitamente. É o que segura a banda: filtrar só na exibição gastaria a mesma
saída de rede. Por isso cada tela aparece primeiro como um convite
("Assistir tela") em vez de já começar a tocar.

## Salas

- **No Discord:** não há lista. A atividade entra direto na sala daquela call.
  Com `DISCORD_BOT_TOKEN` configurado, o servidor confirma com o Discord quem
  está no canal de voz; sem ele, o escopo é a instância da atividade.
- **No site:** não existe call para herdar, então a lista de salas é a única
  forma de as pessoas se encontrarem. Salas podem ter senha.

Salas vivem em memória e fecham sozinhas 12 segundos depois de esvaziar — a
carência existe porque recarregar a página desconecta e reconecta.

## Som

O áudio vai pelo mesmo socket e pelo mesmo cabeçalho do vídeo, distinguido só
pelo byte de tipo. Opus a 96 kbps, capturado junto com a tela por
`getDisplayMedia({ audio: { systemAudio: 'include' } })`.

**O som só sai de aba.** Compartilhar a tela inteira entrega a mistura do
sistema, com a saída do Discord dentro — e a call inteira passa a se ouvir de
volta. Não existe API para tirar um processo dessa mistura: o áudio é capturado
por processo e a relação com uma janela não é um-para-um. O que dá para saber é
o `displaySurface` escolhido, e isso basta — `browser` significa som daquela
aba só. Nos outros casos a faixa é parada antes de sair da máquina.

Junto vai `restrictOwnAudio` quando o navegador suporta: ele tira da captura o
que a própria página está tocando, senão quem transmite enquanto assiste devolve
o som da outra tela para a sala, em laço.

Três coisas que o desenho assume:

- **Áudio não tem keyframe.** Cada pacote Opus se decodifica sozinho, então ele
  não passa pelo bloqueio que barra vídeo sem ponto de partida. Se passasse,
  quem entra no meio ficaria mudo até o próximo keyframe.
- **Buraco em áudio é audível.** Um quadro de vídeo perdido não se nota; um
  intervalo sem amostra é um estalo. Por isso a reprodução mantém um colchão de
  80 ms — o som toca um pouco atrás do vivo, e essa folga absorve o solavanco
  da rede. Passando de 320 ms acumulados, corta e volta ao vivo: atraso somado
  não se recupera sozinho.
- **Sincronia é aceitável, não exata.** O vídeo é desenhado assim que chega; o
  som carrega o colchão. A diferença fica em algumas dezenas de milissegundos,
  abaixo do que se percebe em tela de computador. Casar os dois exigiria
  atrasar o vídeo até o áudio — mais latência para resolver um problema que não
  aparece fora de rosto falando.

A reprodução agenda cada pedaço num `AudioBufferSourceNode`, sem AudioWorklet.
O worklet daria precisão por amostra, mas exige um arquivo carregado por URL, e
dentro da atividade toda URL passa pelo proxy do Discord — um caminho a mais
para dar errado, em troca de precisão que pacotes de 20 ms não pedem.

## Protocolo

Cada pacote trafega como binário puro:

```
[1B slot][1B tipo: 1=vídeo completo 2=vídeo parcial 3=som][8B tempo][8B relógio][payload]
```

O `slot` é o número do transmissor, carimbado na origem: o servidor repassa o
buffer sem tocar nele, e quem assiste sabe para qual decodificador mandar. Até
4 transmissores por sala.

O relógio de envio serve só para medir atraso. É exato na mesma máquina; entre
máquinas diferentes, aproximado.

Controle vai em JSON: `start`, `config`, `audio-config`, `stop`, `rtc`,
`quality` (transmissor → servidor); `state`, `stream-start`, `config`,
`audio-config`, `stream-stop`, `need-keyframe`, `rtc-want`, `rtc`, `rtc-bye`,
`chunks`, `quality-down`, `quality-up`, `error` (servidor → clientes);
`watch`, `unwatch`, `rtc`, `rtc-ativo` (espectador → servidor).

## Qualidade adaptativa

O servidor é o único lado que enxerga todos os espectadores ao mesmo tempo.
Quem transmite não sabe que a conexão de alguém está engasgando; quem assiste
não sabe se o problema é só dele. O relay sabe as duas coisas, e é por isso que
ele decide **quando** a qualidade precisa ceder — enquanto o transmissor, dono
do codificador, decide **quanto**.

A cada 4 segundos, junto da limpeza de salas, o servidor classifica cada
transmissão em uma de três situações:

- **suja** — algum espectador que depende do relay derrubou 2 ou mais quadros
  na janela. Sai um `quality-down`, no máximo um a cada 2 s;
- **limpa** — existe espectador pelo relay e nenhum deles sofreu. Duas janelas
  limpas **consecutivas**, mais 10 s desde o último ajuste, devolvem um degrau
  com `quality-up`;
- **sem evidência** — não há espectador nenhum pelo relay (todos migraram para
  a conexão direta, ou ninguém está assistindo). Isso não é prova de saúde, é
  ausência de prova: a sequência de janelas limpas **zera**, e nada é ajustado.

A assimetria é deliberada: descer custa uma janela, subir custa duas janelas
limpas seguidas e dez segundos. É essa histerese que impede o laço de oscilar
com um espectador instável — entre um `quality-down` e o `quality-up` seguinte
passam no mínimo três janelas.

O transmissor aplica o degrau sobre a **escolha da pessoa**, nunca sobre o
valor anterior: primeiro o bitrate cai 25% por degrau até o piso de 300 kbps, e
só então a taxa de quadros desce (60 → 30 → 24 → 18 → 15). A qualidade efetiva
é sempre derivada do par (teto escolhido, degraus), então o ajuste automático
não tem como ultrapassar o que a pessoa pediu — não existe caminho de código
que suba acima do teto.

Depois de cada mudança — automática ou manual — o transmissor responde com um
snapshot:

```json
{ "type": "quality", "degraus": 3, "bitrate": 1054688, "fps": 30, "piso": false }
```

O servidor **espelha** esse relato em vez de manter contagem própria. Se ele
contasse sozinho, teria a mesma escada escrita em dois lugares, livres para
divergir — e a divergência apareceria como dívida de `quality-up` que nunca
fecha, ou como `quality-down` insistindo com quem já está no piso. Com o
snapshot, `piso: true` faz o servidor parar de pedir o que não há como ceder, e
a recuperação é finita por construção. Um transmissor que nunca reporta
simplesmente nunca recebe `quality-up`: falha fechando, nunca acima do teto.

O primeiro snapshot sai logo depois do `start` e **antes do primeiro quadro**.
A ordem importa: sem ele o relay ainda não conhece a taxa da transmissão, trata
a janela fria pelo orçamento mínimo em bytes e recusaria o primeiro keyframe de
uma tela grande — que passa de centenas de KB.

**Limitação assumida, a mesma do Discord:** o codificador é um só, então
socorrer um espectador reduz a qualidade para todos. A saída estrutural — uma
qualidade por espectador — exige codificar N vezes, que é o que um SFU de
verdade faz e está fora do escopo deste projeto. Quem tem conexão boa e não
quer pagar pelo vizinho já tem a saída que existe: a conexão direta por WebRTC,
que tira aquele espectador do relay e do cálculo.

## WebRTC por cima do relay

O relay acima é o piso, e continua sendo o caminho de todo mundo no primeiro
segundo. Por cima dele, cada espectador ganha uma tentativa de conexão direta
com quem transmite.

A diferença que importa não é o número de saltos — é o transporte. O WebSocket
anda sobre TCP, e TCP não sabe descartar um quadro atrasado: quando a rede
aperta, ele entrega tudo, em ordem, mais tarde. A imagem não fica pior, ela
fica no passado, e o que se vê é a transmissão andando aos saltos. O WebRTC
anda sobre SRTP/UDP: abaixa o bitrate sozinho quando detecta perda, repõe
pacote perdido com NACK e, no limite, deixa o quadro velho para trás. Ele
degrada a qualidade em vez de degradar o tempo.

Como funciona a troca:

1. Alguém pede `watch`. O relay começa a entregar na hora, como sempre fez, e
   o servidor manda um `rtc-want` ao transmissor com o nome daquele espectador.
2. O transmissor abre um `RTCPeerConnection`, pendura as faixas do stream que
   já está capturando e manda a oferta. Quem tem a mídia é quem oferece.
3. Offer, answer e candidatos ICE viajam como envelopes opacos pelo mesmo
   socket do relay — ele já existe e já está autenticado.
4. Quando o primeiro quadro **aparece de fato** no `<video>` do espectador — e
   não quando a conexão diz "connected" —, ele avisa `rtc-ativo`. Só então o
   servidor para de mandar os bytes daquela tela para ele.
5. Se todo mundo que assiste chegou nesse ponto, o servidor manda `chunks:
false` e o transmissor para de codificar para o relay: aqueles quadros não
   teriam para onde ir, e a subida dele agora é disputada pelas conexões
   diretas.

E quando não fecha — NAT simétrico sem TURN, sandbox que bloqueia, rede
corporativa — nada acontece. Passados 8 segundos sem quadro, ou na primeira
falha de ICE, o espectador desiste em silêncio e segue no relay, que nunca foi
desligado para ele. É por isso que o WebCodecs não saiu do código: ele é o que
garante que ninguém fica sem imagem por causa de um roteador.

`TURN_URL`, `TURN_USER` e `TURN_PASS` no `.env` (opcionais) alimentam o
`/api/ice`. Sem eles fica só o STUN público, que resolve a maioria das casas
mas não quem está atrás de CGNAT. Um TURN encaminha o vídeo de verdade — custa
banda, e por isso é escolha de quem hospeda, não padrão.

## Detalhes que não são acidentais

- **`latencyMode: 'realtime'`** no codificador e **`optimizeForLatency: true`**
  no decodificador. Sem eles, ambos acumulam quadros antes de emitir — comprime
  melhor, mas é atraso que nunca mais sai.
- **`frame.close()`** depois de desenhar. `VideoFrame` segura memória de GPU;
  sem isso a aba trava em segundos.
- **Descartar quadro quando a fila do codificador passa de 2.** Fila vira
  atraso permanente. Melhor perder um quadro do que carregar o atraso.
- **`track.contentHint = 'text'`.** Avisa que é tela, não vídeo — mantém texto
  nítido em vez de suavizar bordas.
- **Backpressure no relay.** Se o socket de alguém acumula mais de 2 MB, o
  servidor descarta quadros para essa pessoa em vez de enfileirar. Sem isso, um
  espectador com internet ruim derruba o processo por consumo de memória.
- **A troca de transporte é decidida pelo primeiro quadro, não pelo
  `connectionState`.** Um peer "connected" que não entrega nada é
  indistinguível de um travamento — e desligar o relay confiando nele deixaria
  a tela preta com a conexão reportando sucesso.
- **`degradationPreference`.** Tela usa `maintain-resolution`: texto ilegível é
  pior que texto a 10 quadros. Câmera usa `maintain-framerate`, porque ninguém
  lê um rosto e movimento picado incomoda mais que imagem macia.
- **`/.proxy/`** em todo fetch e WebSocket feito de dentro da atividade — é
  assim que o Discord roteia para o seu servidor.
- **Client ID vem do servidor, não do build.** Embutir no bundle obrigava a
  rebuildar a cada troca de credencial, e esquecer disso não dava erro: a
  atividade abria e só quebrava no login.

## A tela branca depois de um deploy

O sintoma: você atualiza o servidor, o site abre normalmente no navegador, e a
atividade no Discord fica um retângulo branco por um tempo longo — sem erro
nenhum no log, com o servidor respondendo 200 a tudo. Depois de um tempo
indeterminado ela volta sozinha.

Não é o mesmo retângulo branco do `X-Frame-Options` descrito em
[vps.md](vps.md): aquele é permanente e vem da borda da hospedagem. Este é
temporário e vem de três peças nossas que só produzem o defeito quando se
encaixam — nenhuma delas é visível olhando para uma só.

**1. Cada build troca o nome dos arquivos.** O Vite escreve
`assets/index-<hash>.js` com hash do conteúdo, e `emptyOutDir: true` apaga o
`dist` inteiro antes de escrever o novo. Terminado o deploy, o bundle anterior
não existe mais em lugar nenhum.

**2. O Discord entrega um `index.html` velho.** O servidor manda `no-store`
nele, e no navegador isso basta. Entre o iframe e a hospedagem, porém, existem
duas camadas que o navegador comum não tem: o proxy `<app-id>.discordsays.com`
e o cache do Chromium dentro do cliente desktop — que sobrevive a fechar e
reabrir a atividade. É a mesma observação que motivou o `checkVersion`. Esse
HTML velho pede o hash velho.

**3. O catch-all devolve HTML no lugar do arquivo que sumiu.** O
`express.static` dá 404 no hash velho, e a rota `app.get('*')` do
`server/index.js` atende qualquer caminho fora de `/api` com o `index.html`.
O pedido de um módulo JS recebe, então, **200 com `Content-Type: text/html`**.
O Chromium recusa executar (`Failed to load module script: expected a
JavaScript module script but the server responded with a MIME type of
text/html`), e o CSS leva o mesmo tratamento. Nenhum script roda, nenhum estilo
aplica: retângulo branco. Do lado do servidor tudo foi 200 — daí o log limpo.

O `checkVersion` foi escrito exatamente para avisar disso, mas não alcança este
caso: ele vive dentro do bundle que não carregou. O aviso "feche e abra de
novo" nunca chega a aparecer.

E o site não sofre porque no navegador existe F5: o `no-store` do `index.html`
é respeitado, vem HTML novo com hashes novos, funciona na hora. Dentro da
atividade não há F5, e o cache que serve aquele HTML não é seu — só resta
esperar ele expirar.

### O conserto

O ponto de ataque é a peça 1, não a 2: o cache do Discord não está sob nosso
controle, mas o nome do arquivo está. **Se o HTML velho pedir um nome que
continua existindo, ele carrega o JS novo** e o problema desaparece na raiz.

```js
// client/vite.config.js
build: {
  outDir: 'dist',
  emptyOutDir: true,
  rollupOptions: {
    output: {
      entryFileNames: 'assets/app.js',
      assetFileNames: 'assets/[name][extname]',
    },
  },
}
```

Nome fixo pede a política de cache oposta: no `server/index.js`, a regra que
carimba `immutable` em tudo que está em `/assets` passa a carimbar `no-store`
nesses dois. O que se perde é o cache eterno do bundle de entrada — irrelevante
num arquivo que muda a cada deploy e que o proxy do Discord ia servir velho de
qualquer jeito.

Dois complementos que valem por si:

- **404 de verdade para caminho com extensão.** Um `if (path.extname(req.path))
  return next()` no começo do catch-all. A rota existe para o roteamento da
  aplicação, não para asset — e devolver HTML no lugar de um `.js` que faltou
  troca um erro visível em três segundos por uma tela branca muda.
- **Build atômico.** Hoje o `npm run build` roda com o servidor antigo ainda no
  ar, e o `emptyOutDir` deixa vários segundos em que o `dist` está vazio e o
  site inteiro responde 404 — inclusive o `/`. Se o proxy do Discord pegar uma
  resposta de erro nessa janela, ele a guarda, e a tela branca dura bem mais
  que o deploy. Montar em `client/dist.novo` e trocar com `mv` fecha a janela.

### Como confirmar que é isto

Abra o devtools da atividade no Discord desktop (Ctrl+Shift+I, com o modo
desenvolvedor ligado) logo depois de um deploy. O erro de MIME type no console
é a assinatura. Do lado de fora dá para ver o mesmo sem o Discord:

```bash
curl -sI https://seu-dominio/assets/index-HASHQUENAOEXISTE.js | head -2
```

Se vier `200` e `content-type: text/html`, é o catch-all respondendo.

## Estrutura

```
server/
  index.js        HTTP + WebSocket, login do Discord, emissão de tokens
  rooms.js        salas e repasse dos quadros
  tokens.js       tokens assinados (sem biblioteca externa)
  public/share.*  a aba de captura, que roda FORA do Discord
client/
  src/main.js     interface da sala e conexão
  src/player.js   decodifica os quadros e desenha no canvas
  src/audio.js    decodifica o som e agenda a reprodução
shared/
  broadcaster.js  captura + codificação, usada pela aba e pela atividade
  rtc.js          conexão direta por WebRTC, por cima do relay
scripts/
  configurar.mjs  assistente de configuração
  tunel.mjs       sobe o túnel e grava o endereço no .env
  smoke.mjs       teste do servidor ponta a ponta, sem navegador
```

## Testes

```
npm start        # numa janela
npm run smoke    # noutra
```

Cobre autenticação, senha de sala e bloqueio por tentativas, a máquina de
estados do keyframe, "assistir é opt-in", vários transmissores sem misturar os
streams, e isolamento entre salas e instâncias.

## Rodando enquanto mexe no código

`npm start` reconstrói o site a cada execução. Para recarregar sozinho a cada
salvamento, use `npm run dev` — ele sobe o servidor na 3001 e o site na 5173,
e é a 5173 que você abre.
