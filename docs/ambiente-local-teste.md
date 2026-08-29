# Ambiente local de teste — porta, QUIC e certificado

Complementa [`webtransport.md`](webtransport.md), que descreve a operação. Este
documento é sobre rodar na sua máquina: três armadilhas que ninguém adivinha
lendo o código, uma delas com prazo de validade.

Montado em 2026-08-27 para teste local com túnel e WebTransport ligado.

## O que está configurado

| item         | valor                      | onde                                     |
| ------------ | -------------------------- | ---------------------------------------- |
| HTTP         | `http://localhost:3100`    | `PORT`, `PUBLIC_ORIGIN` no `.env`        |
| WebTransport | ligado                     | `WEBTRANSPORT_ENABLED=true`              |
| listener UDP | `127.0.0.1:4443`           | `WEBTRANSPORT_HOST`, `WEBTRANSPORT_PORT` |
| certificado  | modo `hash`                | dispensa domínio e CA                    |
| cert/key     | `C:\tmp\locutor-wt-certs\` | fora do repositório                      |

O `.env` está no `.gitignore` e o certificado vive fora da árvore — segredo não
se versiona, e `git diff --check` é gate contratado.

Para saber se o WebTransport realmente subiu, a rota mais barata é
`/api/transports`:

```
GET http://localhost:3100/api/transports
{"websocket":true,"webtransport":{"url":"https://localhost:4443/wt",
 "version":1,"hashes":["mTbz9xRqmZYggEJXSx3SxB2risnGDGfIFZ7b54WSYBA="]}}
```

`webtransport: null` significa desligado ou que o bind falhou; objeto significa
ativo. O log confirma: `[webtransport] listening UDP 127.0.0.1:4443`.

## A porta 3001 não funciona nesta máquina

`npm -w server run start` na porta padrão morre com:

```
Error: listen EACCES: permission denied 0.0.0.0:3001
```

`EACCES` **não** é porta ocupada — isso seria `EADDRINUSE`. A porta caiu numa
faixa que o Windows reservou:

```
netsh int ipv4 show excludedportrange protocol=tcp
   2979  3078      <- engole a 3001
```

Medido em 2026-08-27, dez faixas excluídas no total. A 4443/UDP está livre, e as
listas de TCP e UDP são independentes — conferir as duas separadamente.

**Quem reserva, aqui:** `winnat` (Running). Hyper-V (`vmms`) e WSL
(`LxssManager`) **não estão instalados** nesta máquina. A literatura costuma
atribuir essas faixas a Hyper-V/WSL porque são as causas mais comuns; aqui
nenhum dos dois existe, e desativá-los seria perseguir fantasma.

Não verificado: dizem que as faixas são re-sorteadas a cada boot. Não há
comparação antes/depois para confirmar. Se um dia a 3100 também der `EACCES`, o
primeiro comando a rodar é o `netsh` acima — não é regressão do código.

## O certificado expira em 2026-09-08, e falha em silêncio

O modo `hash` exige ECDSA P-256 com validade total de no máximo 14 dias
(ver [`webtransport.md`](webtransport.md), seção "Certificado Web PKI versus
hash"). O par atual foi gerado com 12:

```
de : Aug 27 08:18:53 2026 GMT
até: Sep  8 08:18:53 2026 GMT
```

E aqui está o perigo, na letra da própria documentação de operação: _"falha de
rotação retira a capability WT em vez de anunciar um certificado divergente"_.

Traduzindo: **quando vencer, o WebTransport simplesmente some.** O servidor sobe
normal, a aplicação funciona, e a mídia volta calada para o relay WebSocket.
Nenhum erro, nenhum aviso, nenhuma linha de log gritando. Se em setembro alguém
notar que "o QUIC parou de ser usado", é isto — e não uma regressão.

O comportamento é deliberado e correto: anunciar hash divergente seria pior. Mas
significa que a expiração é **invisível por design**, e só um check ativo pega.

Para regenerar, gere um par ECDSA P-256 com `notAfter` no máximo 14 dias à
frente, SAN cobrindo `localhost` e `127.0.0.1`. A receita de referência está em
`server/webtransport-auth-shard-independent.test.js` — usar a mesma do oráculo é
deliberado: se o formato aceito pelo servidor mudar, o teste quebra junto e a
divergência aparece em CI em vez de aparecer em produção.

Depois de regenerar, o hash publicado em `/api/transports` muda. **Conferir que
o novo hash bate com o do certificado gerado é a prova de que a rotação pegou.**

## O túnel não carrega QUIC

`npm run tunel` sobe um túnel descartável (endereço novo a cada execução);
`npm run tunel:criar` cria um com endereço fixo. Ambos usam cloudflared, baixado
automaticamente, e gravam o endereço no `.env`.

Mas, de [`webtransport.md`](webtransport.md): _"um proxy ou túnel configurado
somente para HTTPS/TCP não transporta QUIC"_.

| caminho   | WebRTC P2P | relay WS | WebTransport/QUIC |
| --------- | ---------- | -------- | ----------------- |
| localhost | sim        | sim      | **sim**           |
| túnel     | sim        | sim      | **não**           |

Não é limitação do setup — é exatamente a razão pela qual o relay WebSocket
precisa existir, já que Activities e PaaS costumam não oferecer UDP.

Para medição repetida, `tunel:criar` poupa trocar o "Target" no portal do
Discord a cada reinício.

## O que não se sabe sobre produção

- **Hospedagem não definida.** Se o host não passar UDP, ligar
  `WEBTRANSPORT_ENABLED` não adianta: QUIC é UDP.
- **TURN não configurado** (`TURN_URL=` vazio). Sem ele o atalho WebRTC falha em
  NAT simétrico, e essas pessoas caem no relay — que é justamente onde o QUIC
  está desligado por padrão.
- `.env.example` mantém `WEBTRANSPORT_ENABLED=false`. O que foi ligado aqui é o
  `.env` **local**, não o padrão do projeto.
