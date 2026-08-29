# Operação do WebTransport

WebTransport é uma aceleração opcional do relay. WebSocket permanece obrigatório
e universal: com a opção desligada, addon ausente, configuração inválida, bind UDP
recusado ou listener perdido, o HTTP/WS continua de pé e a capability anuncia
`webtransport: null`.

## Rede e dependências

Os pacotes `@fails-components/webtransport` e
`@fails-components/webtransport-transport-http3-quiche` são dependências opcionais
do workspace `server`. Não os torne imports estáticos. O listener HTTP/3 usa
UDP/QUIC, portanto `WEBTRANSPORT_PORT` precisa chegar por UDP ao processo; um
proxy ou túnel configurado somente para HTTPS/TCP não transporta QUIC.

Instale normalmente para habilitar o path live. A instalação inerte contratada é:

```text
npm ci --omit=optional
npm run verify:ws-only
```

## Variáveis

| Variável | Uso |
| --- | --- |
| `WEBTRANSPORT_ENABLED` | `true`/`1` habilita o carregamento opcional e o bind. |
| `WEBTRANSPORT_HOST` | Interface local do listener UDP; obrigatória em produção. |
| `WEBTRANSPORT_PORT` | Porta UDP; obrigatória e não zero em produção. |
| `WEBTRANSPORT_PUBLIC_URL` | URL HTTPS limpa do mesmo nó, terminando em `/wt` (e preservando `/.proxy`/`/nK` quando aplicável). |
| `WEBTRANSPORT_CERT_MODE` | `webpki` ou `hash`. |
| `WEBTRANSPORT_CERT_PATH` | Certificado current em PEM. |
| `WEBTRANSPORT_KEY_PATH` | Chave privada current em PEM. |
| `WEBTRANSPORT_NEXT_CERT_PATH` | Certificado next opcional, sempre junto da chave next. |
| `WEBTRANSPORT_NEXT_KEY_PATH` | Chave privada next opcional. |

Use caminhos e domínios reais apenas no `.env` local. Os exemplos do repositório
são deliberadamente fictícios e nenhum certificado ou segredo deve ser versionado.

## Certificado Web PKI versus hash

Em `webpki`, use um certificado público cuja cadeia, hostname e SAN sejam válidos
para `WEBTRANSPORT_PUBLIC_URL`. A factory não envia hashes nesse modo; a validação
normal do navegador continua responsável pela confiança.

Em `hash`, current e next precisam ser ECDSA P-256 e ter validade total de no
máximo 14 dias. A capability publica hashes SHA-256 e a factory usa
`allowPooling:false`. Quando o par next está configurado, o servidor publica
current+next, mantém uma sobreposição de três segundos, passa a servir next e só
então remove o hash antigo. Prepare um novo par antes de cada vencimento; falha
de rotação retira a capability WT em vez de anunciar um certificado divergente.

## Sharding, autenticação e diagnóstico

Cada nó anuncia apenas a própria URL WT. A Activity preserva `/nK`; token, origem,
path e shard são validados antes de anexar sessão ou sala. O GET de capability não
leva query/token e o CONNECT copia somente `t`, `fonte` e `modo`. Nunca registre
essas queries.

Os estados esperados no servidor são `disabled`, `misconfigured`, `listening`
(com host/porta) e `listener-lost`. No cliente, `transport` só passa a
`webtransport` depois do handshake; timeout ou falha anterior a `OPEN` abre no
máximo um WS. Falha posterior fecha a sessão e deixa cada papel aplicar seu
lifecycle normal, sem hot-swap silencioso.

## Mídia híbrida e recuperação

O controle permanece em um stream bidirecional enquadrado. Keyframes de vídeo
continuam confiáveis; deltas e áudio seguem por datagramas fragmentados para que
um quadro vencido não consuma a rede com retransmissão. Após perda nativa
observada, deltas de vídeo recebem temporariamente uma paridade XOR; a proteção
desarma depois da janela mínima e de amostras limpas, evitando overhead permanente.

Cada lane mantém sequência, teto de assemblies e orçamento físico dos bytes
retidos. Um gap causado por datagrama vence em 250 ms. Se um keyframe confiável já
estiver pendente atrás do buraco, ele vira imediatamente a nova âncora, sem ser
descartado para esperar outro ciclo de rede. Se ainda não houver âncora, o pedido
`need-keyframe` chega ao broadcaster e se repete de forma limitada até a retomada.
Pressão/perda nativa também alimenta a descida de bitrate, resolução e FPS; a
subida usa histerese para não oscilar depois que a rede volta.

## Gates reproduzíveis

```text
npm run test:webtransport
npm run verify:ws-only
```

O primeiro usa somente addons do `node_modules` deste workspace, gera certificados
P-256 em diretório temporário nos próprios testes live, escolhe portas efêmeras,
exercita listener/capability/streams/relay/stress/rollover e limpa processos,
arquivos e portas. O segundo copia a árvore atual, instala com
`npm ci --omit=optional` e prova testes, build, start e smoke no modo WS-only.
