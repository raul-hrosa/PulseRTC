# PulseRTC — Signaling + SFU

Uma plataforma própria de comunicação em tempo real, construída incrementalmente.
Um servidor em Go que faz **signaling** (WebSocket, rooms, participantes) e
também é um **SFU (Selective Forwarding Unit)**: cada navegador tem **uma**
`RTCPeerConnection` com o servidor, que **encaminha** o RTP de um publisher
para os demais participantes da sala — **sem transcodificar**.

## Visão geral das capacidades

- **Signaling Server** — WebSocket, rooms, participantes, eventos, forward de sinalização.
- **WebRTC P2P** entre dois navegadores (getUserMedia, `RTCPeerConnection`,
  SDP offer/answer, ICE candidates, remote track, teardown) — caminho legado, hoje coexistindo com o SFU.
- **Observabilidade RTP/RTCP**: métricas via `getStats()` (`web/stats.js`),
  dashboard de diagnóstico e docs de fundamentos de mídia.
- **SFU (1 Publisher → N Subscribers → N Publishers → N Subscribers)**. Pacote
  `internal/sfu` (Pion WebRTC v4): uma `PeerConnection` por participante, RTP
  forwarding sem transcode, RTCP via interceptors do Pion, pub/sub seletivo,
  perfect negotiation, endpoint `GET /sfu/stats`.
- **Media Quality / QoE Engine** (`internal/quality`): o navegador reporta
  métricas WebRTC (`quality_report`, ~1/s); o servidor deriva taxas, aplica
  janela + análise (áudio/vídeo/conexão) + histerese, e produz status
  (GOOD/WARNING/POOR/UNKNOWN) + score + problemas + eventos
  (`quality_degraded`/`quality_recovered`). `GET /quality?room=`. Docs:
  `docs/perf/qoe.md`, `docs/perf/quality-model.md`, `docs/perf/metrics.md`.
- **Load testing & performance**: `cmd/loadtest` (clientes Pion, não abas de
  navegador), métricas Prometheus em `/metrics`, `/debug/pprof`. Baseline de
  performance em `docs/perf/benchmark-report.md`.
- **Security & Authentication**: **fronteira de confiança** do PulseRTC.
  Pacote `internal/auth` (JWT HMAC, validação de assinatura / `exp` / `iat` /
  `iss` / `aud` / janela de replay), modelo explícito de permissões
  `JOIN/PUBLISH/SUBSCRIBE/CONTROL`, autorização de room pelo claim `room`,
  identidade derivada do token (cliente não escolhe seu `participantId`),
  autenticação do WebSocket **antes** do upgrade, expiração de token em sessão
  ativa (disconnect gracioso), rate limiting básico (conexões/IP e
  mensagens/conexão), limite de tamanho de mensagem, endpoints HTTP protegidos
  (`/health` continua público), métricas `auth.*` em `/metrics`. Tudo por
  ambiente (`PULSERTC_*`). Docs: **`docs/security/authentication.md`**.
- **Arquitetura distribuída**: base para **múltiplos nós** sem mover mídia
  entre eles por padrão. Pacote `internal/cluster` (interfaces + backend
  in-memory ou Redis): identidade de nó, `NodeRegistry` com heartbeat/stale,
  **room ownership** (o nó que cria a room é o dono; claim atômico → 1 dono),
  `RoomLocator`/`ParticipantLocator`, estado compartilhado via Redis,
  sinalização e mídia cross-node (SFU ↔ SFU sobre UDP dedicado), load
  balancing / room routing, detecção de falha e recuperação de sessão. Join
  numa room de outro nó → erro `ROOM_ON_OTHER_NODE` com o `nodeId`. Nenhuma
  chamada de cluster no caminho de RTP local. `PULSERTC_CLUSTER_ENABLED=false`
  (default) = comportamento single-node idêntico. Docs:
  **`docs/architecture/distributed-architecture.md`**.
- **Integration API (`/v1`)**: camada HTTP pública para uma aplicação externa
  controlar o PulseRTC sem conhecer SFU/cluster/Pion.

O servidor **termina** SRTP/DTLS/ICE e encaminha RTP, mas **nunca decodifica**
a mídia. Continua **sem** simulcast, SVC, gravação, transcoding, TURN próprio
embarcado por padrão, OAuth/OIDC/SSO ou banco de usuários, migração de rooms
entre nós, e multi-tenancy — ver `docs/deploy.md` e a seção
[Problemas encontrados](#problemas-encontrados) / [Melhorias futuras](#melhorias-futuras-identificadas).

---

## Estrutura do projeto

```text
pulsertc/
├── cmd/
│   ├── server/                 # bootstrap: HTTP, rotas, slog, graceful shutdown
│   ├── loadtest/               # cliente de carga (Pion)
│   └── token/                  # gerador de JWT para testes
├── internal/
│   ├── room/                   # presença (control plane): Room + Manager
│   ├── signaling/               # protocolo WebSocket, roteamento, join/leave, /sfu/stats
│   ├── sfu/                    # media plane (Pion WebRTC v4): rooms, fan-out, negotiation
│   ├── quality/                # Quality Engine (QoE) — thresholds, analyzers, histerese
│   ├── auth/                   # JWT HMAC, rate limiting
│   ├── cluster/                # estado distribuído, cross-node signaling/media, recovery
│   ├── api/                    # Integration API pública (/v1)
│   └── loadtest/               # motor de cenários do cmd/loadtest
├── web/                        # cliente pub/sub + dashboards QoE (index.html, app.js, quality.js, stats.js)
├── docs/                       # architecture, networking, perf, api, security, ADRs
├── deploy/                     # docker-compose.redis.yml, turnserver.conf
├── scripts/                    # bench.sh, bench-regression.sh
├── benchmarks/                 # resultados de load test (json), baseline vigente
├── learn/                      # tutorial de Go do zero usando o código do projeto
├── Dockerfile · docker-compose.yml · go.mod
```

Mapa detalhado do código Go — `cmd/` + pacotes de `internal/` (responsabilidade,
dependências, fronteiras): `docs/architecture/internal-packages.md`.

### Decisões arquiteturais

- **`internal/room` não conhece WebSocket.** Depende apenas de uma interface
  `Participant { ID() string; Send([]byte) }`. Isso mantém a lógica de salas
  isolada e trivial de testar, e deixa a porta aberta para outros transportes.
- **Um writer por conexão.** Cada `Client` tem uma goroutine `writePump` e um
  canal `send` bufferizado (32). Gorilla não permite escritas concorrentes no
  socket; o canal serializa tudo e ainda serve de _backpressure_.
- **Cliente lento é desconectado.** Se o buffer `send` enche, a conexão é
  fechada em vez de travar quem faz o broadcast.
- **Rooms vazias são removidas** pelo `Manager` assim que o último participante
  sai — rooms ociosas não vazam memória.
- **Identidade é do servidor.** O `participantId` é um UUID gerado no connect
  (RF03); nada vindo do cliente é usado como identificador.
- **`participant_joined` não é ecoado para quem entrou** — ele já recebe a lista
  completa em `room_joined`. Decisão documentada em `docs/networking/protocol.md`.
- **Erros não fecham a conexão**, para o cliente de teste continuar utilizável.

### Ordem de locks (sem race)

`Manager.mu` → `Room.mu`, nunca o inverso. `Room` usa `RWMutex`
(broadcast/listagem são leitura). Verificado com `go test -race ./...`.

---

## Dependências

| Dependência                  | Uso        | Justificativa |
|------------------------------|------------|---------------|
| `github.com/gorilla/websocket` | WebSocket | Biblioteca WebSocket mais madura e difundida do ecossistema Go, RFC 6455 completa, controle explícito de ping/pong, deadlines e limites de mensagem. API de baixo nível que não impõe framework. |
| `github.com/google/uuid`       | IDs       | Geração de UUID v4 para o `participantId`. Pequena, padrão de fato. |
| `github.com/pion/webrtc/v4` **v4.2.19** | SFU | Implementação WebRTC em Go pura, madura e modular. Dá acesso de baixo nível a `TrackRemote`/`TrackLocalStaticRTP`, `RTPSender`, `WriteRTCP` e `SettingEngine` — exatamente o necessário para construir o forwarding manualmente e **entender** o SFU, sem esconder o mecanismo. Requer **Go 1.24**. |
| `github.com/pion/interceptor` v0.1.47 | RTCP | Interceptors padrão do Pion: Sender/Receiver Reports, gerador/respondedor de NACK, TWCC. É o que mantém o feedback RTCP funcionando através do SFU sem código próprio. |
| `github.com/pion/rtcp` v1.2.17 | RTCP | Tipos de pacote RTCP (`PictureLossIndication`) para encaminhar PLI ao publisher. |
| `github.com/redis/go-redis/v9` | Estado distribuído | Cliente Redis oficial, usado só para coordenação (ownership de room, registro de nó, localização de participante) — nunca no caminho de mídia. |
| `github.com/alicebob/miniredis/v2` | Testes | Redis in-memory para testar o backend distribuído sem infraestrutura externa. |

Nenhum framework HTTP: a `net/http` da stdlib é suficiente para as rotas
(`/`, `/health`, `/ws`, `/sfu/stats`, `/v1/*`).

---

## Protocolo de mensagens

Resumo (detalhes em `docs/networking/protocol.md`):

```text
Client → Server:  join, sfu_offer, sfu_answer, sfu_ice_candidate,
                  subscribe, unsubscribe, unpublish, set_mute,
                  quality_report, session.resume,
                  signal, webrtc_* (legado P2P)
Server → Client:  welcome, room_joined, participant_joined, participant_left,
                  sfu_offer, sfu_answer, sfu_ice_candidate,
                  publication_added, publication_removed, publication_muted,
                  subscription_added, subscription_removed, subscription_failed,
                  quality_degraded, quality_recovered, quality_changed, error,
                  signal, webrtc_* (legado P2P)
```

Toda mensagem é um objeto JSON com no mínimo `{ "type": "..." }`.
`signal`/`webrtc_*` são **encaminhados** a outro participante (o servidor não
interpreta SDP/ICE). Os `sfu_*` **não** são encaminhados: terminam na
`PeerConnection` que o servidor mantém com aquele participante (ver
`docs/architecture/sfu.md`). `GET /sfu/stats?room=<id>` devolve o diagnóstico
do media plane (role, connection state, RTP in/out por participante).

---

## Como executar localmente

Requer **Go 1.24+** (Pion WebRTC v4.2 exige `go >= 1.24`).

```bash
go run ./cmd/server        # sobe em :8090 (HTTP/WS) + :8091/udp (ICE)
```

Acesse `http://localhost:8090` para o cliente de teste.

## Como executar com Docker

```bash
docker compose up --build            # http://localhost:8090
```

Portas: `8090/tcp` (HTTP + WebSocket) e `8091/udp` (`SFU_UDP_PORT`, mídia ICE).
`SFU_NAT_1TO1_IP` (default `127.0.0.1`) é o IP que o SFU anuncia aos navegadores
— use o IP da LAN/público para acessar de outro dispositivo. Runbook completo
de deploy (TLS, TURN, checklist de produção): `docs/deploy.md`.

## Como executar os testes

```bash
go test ./...
go test -race ./...
```

Sem Go instalado, dentro de um container:

```bash
docker run --rm -v "$PWD":/src -v pulsertc-gocache:/go -w /src -e CGO_ENABLED=1 \
  golang:1.24 sh -c "go test -race ./..."
```

Cobrem: criação/entrada/saída/isolamento de rooms, múltiplos participantes,
room vazia, health check, connect com id único, `join`, `room_joined`,
`participant_joined`, `participant_left`, encaminhamento de `signal`, `signal`
para outra room (rejeitado), mensagens inválidas, conexões concorrentes, todo
o pipeline SFU/QoE/auth/cluster e fuzzing dos parsers de entrada não confiável
(JWT, envelope WebSocket, mensagens de cluster).

---

## Exemplo de comunicação entre dois participantes

1. Abra `http://localhost:8090` em duas abas (A e B).
2. Ambas com Room ID `room-123` → **Connect & Join**.
3. A recebe `welcome` (seu id) e `room_joined` (lista vazia).
4. B entra: B recebe `room_joined` com o id de A; A recebe `participant_joined` com o id de B.
5. Em A, cole o id de B no campo **Target**, escreva um texto e **Send**.
6. B recebe `{ "type": "signal", "from": "<id-de-A>", "payload": { "text": "..." } }`.
7. Feche a aba B: A recebe `participant_left` com o id de B.

Fluxo de mensagens:

```text
A ──join room-123──▶ server                 (server ──welcome/room_joined──▶ A)
B ──join room-123──▶ server ──participant_joined──▶ A
                     server ──room_joined(participants:[A])──▶ B
A ──signal(target=B)──▶ server ──signal(from=A)──▶ B
A ◀──participant_left(B)── server            (quando B desconecta)
```

---

## WebRTC P2P

Caminho legado, anterior ao SFU: dois navegadores negociam diretamente entre
si, o servidor só encaminha sinalização.

### Como funciona

- **Backend:** `handleForward` genérico, usado por `signal` e pelos três tipos
  `webrtc_*` (mesma validação: room + target + payload). O servidor só
  encaminha, nunca interpreta a mídia.
- **Protocolo:** `webrtc_offer`, `webrtc_answer`, `webrtc_ice_candidate` (com
  `from` preenchido pelo servidor). Detalhes em `docs/networking/protocol.md`.
- **Frontend (`web/`):** `getUserMedia`, `RTCPeerConnection`, offer/answer,
  trickle ICE com buffer de candidatos, `ontrack` → `<video>` remoto, estados
  de conexão na UI, teardown em `participant_left`, reconexão manual.
- **Testes:** `internal/signaling/webrtc_test.go` — offer/answer/ICE nos cenários
  válido, target em outra room, target inexistente, payload inválido, sem join,
  e uma negociação completa relayada ponta a ponta.

### Fluxo de negociação

1. A entra na room; B entra depois.
2. Servidor envia `participant_joined(B)` para A e `room_joined(participants:[A])` para B.
3. **A é o iniciador** (já estava na room): `createOffer` → `setLocalDescription`
   → envia `webrtc_offer` para B.
4. B: `setRemoteDescription(offer)` → `createAnswer` → `setLocalDescription`
   → envia `webrtc_answer` para A.
5. A: `setRemoteDescription(answer)`.
6. Ambos trocam `webrtc_ice_candidate` conforme os candidatos são coletados
   (candidato recebido antes da remote description é bufferizado).
7. `ontrack` dispara nos dois lados → vídeo/áudio remoto renderizado.
8. `onconnectionstatechange`: `new → connecting → connected`.

### Configuração STUN

`stun:stun.l.google.com:19302` (público, apenas desenvolvimento), em
`web/app.js` na constante `ICE_SERVERS`.

### Como testar (manual, 2 abas)

Chrome/Edge/Firefox recentes. `getUserMedia` exige contexto seguro:
`http://localhost` é aceito; por IP na LAN use `https://` ou as flags de
_insecure origins_ do navegador.

| Teste | Passos | Esperado |
|-------|--------|----------|
| 1 conexão | A e B entram em `room-test` | A vê B, B vê A; status `connected` |
| 2 áudio | A fala / B fala | ouve-se dos dois lados |
| 3 vídeo | — | câmera de cada um aparece no vídeo remoto do outro |
| 4 saída | B clica **Sair** | A recebe `participant_left`, `pc` fecha, vídeo remoto limpo, status `closed` |
| 5 reconexão | B clica **Entrar** de novo | nova `RTCPeerConnection`, chamada restabelecida |
| 6 rooms isoladas | A em `room-a`, B em `room-b` | nenhuma negociação; nenhum vídeo remoto |

### Limitações conhecidas

- **Apenas 2 participantes** neste caminho legado (P2P puro) — malha N-para-N
  é resolvida pelo SFU, não pelo P2P.
- **Sem perfect negotiation / rollback** aqui: o papel de iniciador é fixo
  (quem já estava na room), suficiente para o caso P2P determinístico.
- **Reconexão é manual** (clicar em Entrar), não automática.

---

## Observabilidade RTP/RTCP

Camada de diagnóstico client-side, independente do SFU/backend.

### Como funciona

- **`web/stats.js`:** camada de coleta. `createStatsCollector(pc, render,
  1000)` chama `pc.getStats()` 1×/s, achata o `RTCStatsReport` em
  `{ video, audio, connection }` e calcula bitrate a partir do delta de bytes.
- **`web/app.js` / `web/index.html`:** dashboard "WebRTC Diagnostics" —
  connection/ICE state, RTT, candidate pair, e por mídia: packets
  sent/received/lost, bytes, frames, resolução, FPS, jitter (áudio), bitrate.
  A coleta inicia em `connectionState === "connected"` e para no teardown.
- **`docs/`:** `rtp.md`, `rtcp.md`, `webrtc-stats.md`, `networking.md`,
  `experiments.md` — conceitos (RTP header, sequence number, timestamp, SSRC,
  payload type/codecs, RTCP SR/RR/NACK/PLI/FIR/TWCC, packet loss, jitter,
  bitrate, RTT) e roteiro dos experimentos de rede.

### Fórmula de bitrate

```text
bitrate_bps = (bytes_agora - bytes_anterior) * 8 / (t_agora - t_anterior)[s]
```

`t` do `performance.now()` na coleta; `* 8` bytes→bits; exibido em kbps
(÷ 1000). Primeira amostra sem anterior → `—`. Detalhes em
`docs/networking/webrtc-stats.md`.

### Como testar

`docker compose up --build` → duas abas em `http://localhost:8090` → mesma Room
→ com a chamada `connected`, o painel atualiza a cada segundo. Para os
experimentos de latência/perda, usar **Chrome DevTools → Network conditions**
ou `clumsy`/Network Link Conditioner/`tc netem` (sem simulador próprio) e
registrar no template de `docs/perf/experiments.md`.
Ferramenta nativa complementar: `chrome://webrtc-internals`.

### Limitações conhecidas

- Campos de `getStats()` variam por navegador/versão — o dashboard mostra `—`
  para ausentes; testado com foco em Chrome/Edge.
- Os experimentos de rede dependem de ferramenta externa de throttling e devem
  ser preenchidos manualmente em `docs/perf/experiments.md`.

---

## SFU (Selective Forwarding Unit)

O componente central de mídia: 1 `PeerConnection` por participante, RTP
encaminhado sem decodificar, modelo pub/sub completo (N Publishers → N
Subscribers).

### Como funciona

- **Pacote `internal/sfu`** (Pion WebRTC v4.2.19): `SFU` → `Room` →
  `Participant` (uma `*webrtc.PeerConnection` por participante) →
  `Publication` (**uma track**, id próprio `pub-<uuid>`) → `Subscription`
  (explícita e seletiva). O forwarding copia RTP com `remoteTrack.Read` →
  `localTrack.Write` (bytes intactos, **sem** decode).
- **`internal/signaling`:** `handleMessage` roteia `sfu_offer`/`sfu_answer`/
  `sfu_ice_candidate` para a `PeerConnection` do participante (não encaminha).
  `handleJoin` cria o participante no SFU; `disconnect` chama `sfu.Leave`.
  `Client` implementa `sfu.Transport`.
- **`GET /sfu/stats?room=<id>`:** diagnóstico do media plane — role
  (publisher/subscriber), connection/ICE state, publications (id, kind, ssrc,
  muted), subscriptions (publication ids), RTP inbound/outbound por participante.
- **Rede ICE:** `SettingEngine` com UDP mux em porta única (`SFU_UDP_PORT`) e
  `SetNAT1To1IPs(SFU_NAT_1TO1_IP)` — para o SFU ser alcançável através do
  mapeamento de portas do Docker.
- **Perfect negotiation** (MDN): browser e SFU podem enviar `sfu_offer`; SFU é
  o peer *impolite* (ignora offer em glare), browser *polite* (rollback +
  retry). Permite *dynamic publish* (browser adiciona track) coexistir com
  *subscribe* (SFU adiciona track).
- **Subscription seletiva:** `subscribe`/`unsubscribe {publicationId}`. Default
  ao conectar: auto-subscribe em tudo; `unsubscribe` grava opt-out
  (`Participant.optedOut`), sem re-subscribe automático.
- **Dynamic publish/unpublish:** `OnTrack` novo → `publication_added` +
  broadcast; fim do ingest loop **ou** `unpublish {publicationId}` →
  `publication_removed` + limpeza das subscriptions.
- **Mute/Camera:** `set_mute {publicationId, muted}` → `publication_muted`
  (metadado; forwarding não para). Distinção *track desabilitada* × *publication
  removida* documentada em `docs/architecture/pubsub.md`.
- **Sinal de falha de mídia:** um write loop de subscriber que falha
  persistentemente (peer travado/SRTP quebrado) desiste após um orçamento de
  erros, encerra a subscription e emite `subscription_failed` ao cliente — em
  vez de vazar silenciosamente.
- **Frontend (`web/`):** 1 `RTCPeerConnection` com o servidor, publica
  áudio+vídeo, tiles de vídeo remoto por participante (agrupados por
  StreamID), badges de mute, botões 🎤 Mute / 📹 Camera Off / ⛔ Unpublish,
  lista de Publications com checkboxes (subscribe seletivo), painel
  `/sfu/stats` + `getStats` local, perfect negotiation no cliente.
- **Observabilidade:** `log/slog` (JSON) com eventos `participant_connected`,
  `participant_joined`, `peer_connection_created`, `track_published`,
  `track_subscribed`, `track_unpublished`, `track_unsubscribed`,
  `peer_connection_closed`, `participant_left`.

### Fluxo de uma Track (objetivo educacional)

```text
Publisher A  ──RTP──►  pc.OnTrack  ──►  Publication.local (TrackLocalStaticRTP)
                                             │  (fan-out: mesmos bytes)
                          ┌──────────────────┼──────────────────┐
                    pc.AddTrack        pc.AddTrack         pc.AddTrack
                          ▼                  ▼                   ▼
                   Subscriber B       Subscriber C        Subscriber …
```

1. **publicada:** browser `addTrack` → `sfu_offer` → SFU `OnTrack` →
   `newPublication` (mint `pub-<uuid>`) + `TrackLocalStaticRTP` com o **mesmo**
   codec.
2. **identificada:** `PublicationID` (server), distinto de `participantId`,
   `remoteTrackID` e `SSRC` (in ≠ out; SSRC é transporte, não identidade).
3. **assinada:** `subscribe {publicationId}` → `Room.findPublication` (na room) →
   `pc.AddTrack(pub.local)` no subscriber → renegociação (`sfu_offer` do SFU).
4. **Subscription criada:** `Participant.subscriptions[pubID]`, RTCP loop,
   `subscription_added`.
5. **RTP encaminhado:** `ingestLoop` copia `remote.Read` → `pub.local.Write`; o
   Pion re-empacota por subscriber (novo SSRC, novas seq).
6. **RTCP:** interceptors do Pion (SR/RR, NACK, TWCC); PLI/FIR do subscriber é
   encaminhado ao publisher; `keyframeLoop` pede keyframe a cada 3 s.
7. **removida:** `publication_removed` → cada subscriber `unsubscribe` +
   `RemoveTrack` + renegociação; sem goroutines órfãs.
8. **múltiplos Publishers/Subscribers:** cada `OnTrack` roda o mesmo fan-out;
   nenhuma `PeerConnection` global.
9. **sem transcode:** a mídia permanece codificada — o SFU só move bytes.

Detalhes: `docs/architecture/sfu.md`, `docs/architecture/architecture.md`, `docs/architecture/pubsub.md`.

### Testes

- **`internal/sfu/*_test.go`** — rooms (criar/entrar/sair/GC), isolamento entre
  rooms, `Publication` (add/remove/list, ids distintos p/ áudio e vídeo),
  `findPublication`/`Subscribe` inexistente → `errNotFound`, stats de room
  inexistente, orçamento de erro do write loop.
- **`internal/sfu/integration_test.go`** — **e2e real** com `PeerConnection`s
  Pion como "navegadores" e perfect negotiation:
  - *PublisherToTwoSubscribers* — A publica vídeo → B e C recebem; C entra
    depois e recebe a publicação já existente; ≥ 10 pacotes RTP efetivamente
    encaminhados são lidos; `/sfu/stats` distingue os roles; B sai e A/C
    continuam.
  - *SelectiveSubscription* — B faz `Unsubscribe` do áudio antes de conectar →
    recebe só vídeo; depois `Subscribe` do áudio.
  - *DynamicUnpublish* — A `Unpublish` do vídeo → B recebe `publication_removed`
    e sua subscription é limpa.
  - *MultiPublisher* — A e B publicam (ids distintos); C recebe os dois.
  - *CrossRoomSubscriptionRejected* — `Subscribe` a publication de outra room
    → `errNotFound`.

### Testes manuais (`http://localhost:8090`, 2-3 abas)

| Teste | Ação | Esperado |
|---|---|---|
| 1 — dois participantes | A e B entram, ambos publicam | A vê B e B vê A (áudio+vídeo) |
| 2 — três participantes | A, B, C publicam | cada um recebe os outros dois |
| 3 — subscription seletiva | C desmarca "B vídeo" | C deixa de receber o vídeo de B |
| 4 — mute | A clica 🎤 Mute | tile de A em B mostra 🔇; unmute volta |
| 5 — camera off | A clica 📹 Camera Off | tile mostra 📷🚫; publication permanece |
| 6 — unpublish | A clica ⛔ Unpublish vídeo | publication some, B para de receber |
| 7 — re-publish | A clica ▶ Publish vídeo | nova publication, B pode assinar |
| 8 — rooms isoladas | A/B em `sala1`, C/D em `sala2` | nenhuma mídia cruza |
| 9 — novo subscriber | B sai, C fica, D entra | D recebe a publicação existente de A |

`getUserMedia` exige contexto seguro: `http://localhost` é aceito; por IP na LAN
use `https://` (ou as flags de _insecure origins_) e ajuste `SFU_NAT_1TO1_IP`.

### Limitações conhecidas

- Sem simulcast, SVC, gravação, transcoding, egress/ingress.
- Sem TURN embarcado por padrão: NAT restritivo do lado do browser pode não
  conectar sem configurar coturn (ver `docs/deploy.md`).
- Campos de `getStats()` variam por navegador.

---

## Media Quality / QoE Engine

### Como funciona

- **Pacote `internal/quality`** — engine pura (sem rooms/WebSocket/SFU):
  - `thresholds.go` — `Config` com **todos** os números (thresholds warn/poor
    por métrica e kind, `WindowSamples`, `DegradeSamples`/`RecoverSamples`,
    faixas de score). Nada de número mágico no código.
  - `metrics.go` — `Sample` (contadores **cumulativos** + instantâneos, todos
    opcionais via ponteiro) → `derive()` → `Derived` (loss %, bitrate, jitter
    ms, rtt ms, fps, frame drop %). Reset de contador descartado.
  - `analyze.go` — `AudioQualityAnalyzer` / `VideoQualityAnalyzer` /
    `ConnectionQualityAnalyzer` **rule-driven** (mesma engine, rule sets
    diferentes). `score.go` — penalidade linear warn→poor, pesos documentados.
  - `history.go` — janela (média das últimas N amostras: pico isolado nunca
    classifica) + ring de status estável.
  - `hysteresis.go` — `stabilizer`: status estável só muda após degradação (3)
    ou recuperação (5) consistente.
  - `engine.go` — `Ingest(Sample) []Event`, `Snapshot(id) ParticipantQuality`
    (connection + outbound[] + inbound[] + **overall** + timeline), `Forget`,
    `Prune`. `overall` não é média: vídeo POOR sozinho → WARNING, não POOR.
  - `events.go` — `quality_degraded` / `quality_recovered` / `quality_changed`
    só em transição de status **estável**.
- **Direção obrigatória:** cada verdito é por `(participante, direção, kind,
  track)`. `outbound` = participante→SFU, `inbound` = SFU→participante — permite
  "A→SFU GOOD, SFU→A POOR".
- **Signaling:** `quality_report` (client→server) → `internal/signaling/quality.go`
  valida, converte e alimenta a engine; eventos voltam **só ao participante**
  (privacidade). `GET /quality?room=` no `main.go`. `Forget` no disconnect.
- **Sessões em recuperação** (ver [Detecção de falha e recuperação](#detecção-de-falha-e-recuperação-de-sessão))
  são marcadas com `SetRecovering`: o Snapshot reporta status=WARNING com
  reason `SESSION_RECOVERY` em vez de um veredito POOR causado pela queda de
  conexão do takeover.
- **Frontend:** `web/quality.js` — coleta `getStats()` da PeerConnection do SFU
  (outbound-rtp + remote-inbound-rtp para loss/rtt, inbound-rtp, candidate-pair)
  1×/s, envia `quality_report`; painel QoE (`/quality` @ 2s) com badges 🟢🟡🔴⚪,
  score, métricas, problemas e mini-timeline; eventos no log.
- **Docs:** `docs/perf/qoe.md` (QoS×QoE, pipeline, analyzers, overall),
  `docs/perf/quality-model.md` (todos os thresholds + histerese + agregação),
  `docs/perf/metrics.md` (cada métrica, fórmula e limitações de navegador).

### Objetivo educacional

```text
WebRTC Stats → Packet Loss / Jitter / RTT / Bitrate / FPS / Frame Drops
            → Quality Analysis (Audio / Video / Connection)
            → Overall Quality → Diagnosis
```

O sistema não mostra só uma cor: `Video POOR` vem **com** `HIGH_PACKET_LOSS`.

### Testes

- **`internal/quality/analyze_test.go`** — packet loss (normal/moderado/alto/sem
  dados), FPS+bitrate baixos, jitter/RTT, track desabilitada, conexão
  `failed`, e o caso `0 0 0 8 0 0` que **não** pode ser POOR (janela).
- **`internal/quality/hysteresis_test.go`** — sem flap em oscilação pequena;
  degrada só após 3 consistentes; recupera só após 5.
- **`internal/quality/engine_test.go`** — sequência
  GOOD→WARNING→POOR→WARNING→GOOD emitindo `quality_degraded`/`quality_changed`/
  `quality_recovered` corretamente; sem evento sem mudança estável; isolamento
  entre participantes; multi-track (áudio GOOD + vídeo POOR → overall WARNING);
  `Forget`.
- **`internal/signaling/quality_test.go`** — `quality_report` de conexão
  `failed` → evento POOR ao cliente; exige room.

### Experimentos de rede (manuais)

Roteiro em `docs/perf/experiments.md` (throttling externo: Chrome DevTools / clumsy /
`tc netem`). Esperado: perda introduzida → `Video`/`Connection` vão a
WARNING/POOR com `HIGH_PACKET_LOSS`/`HIGH_RTT`; rede normalizada → o sistema
reconhece a recuperação (POOR→WARNING→GOOD) e emite `quality_recovered`.

### Limitações conhecidas

- Fonte única: métricas do navegador. `Source` (`browser`/`server`) já existe
  no modelo para acrescentar SFU/rede depois sem reescrever.
- `remote-inbound-rtp` (loss/RTT do outbound) só aparece após o 1º RTCP RR.
- `trackIdentifier` ausente em navegadores antigos → inbound daquele kind vira
  um verdito agregado.
- Sem MOS/ML, sem armazenamento permanente de métricas, sem alertas.

---

## Load testing & performance

### Como funciona

- **`cmd/loadtest` + `internal/loadtest`** — cliente de carga em Go usando a
  mesma stack Pion do SFU (não abas de navegador): conecta ao signaling, entra
  na room, publica áudio+vídeo sintéticos, assina tracks, coleta stats, sai.
  Cenários reproduzíveis (`--scenario baseline|1pub-5sub|10pub-10sub|…`),
  ramp-up/ramp-down, join-leave.
- **`/metrics`** — Prometheus text exposition (Content-Type `text/plain`):
  build info, goroutines, heap, GC, CPU do processo Go, e contadores do SFU
  (rooms/PeerConnections/tracks/publications/subscriptions ativos e cumulativos,
  RTP, rede, negociação — incl. um histograma de duração de negociação).
  **`/metrics.json`** mantém o payload JSON legado (compatibilidade com
  `cmd/loadtest`). **`/debug/pprof/`** habilitado.
- **Gate de regressão**: `scripts/bench.sh` (matriz completa),
  `scripts/bench-regression.sh` + `cmd/benchcheck` comparam um run fresco
  contra `benchmarks/baseline/` (tolerância de 25% em latência de mídia, CPU e
  heap), rodável sob demanda ou via CI noturno
  (`.github/workflows/bench.yml`). Ver `docs/perf/benchmark-report.md`.

### Otimizações aplicadas

- **Limitador de concorrência de negociação** (semáforo global, largura
  `SFU_NEGOTIATION_CONCURRENCY` = `GOMAXPROCS`) envolvendo só o trecho de
  parsing de SDP; + debounce de 8ms em `Participant.scheduleNegotiate` que
  coalesce subscribe/unsubscribe próximos numa única renegociação.
- **Buffer NACK** ajustável (`SFU_NACK_BUFFER_SIZE`, default 256 — 1/4 do
  default do Pion), maior gargalo de heap sob carga.
- **Correção de contador**: `Participant.close()` contabiliza as próprias
  subscriptions em `subscriptionsRemoved`.

Resultado (burst de 20 participantes, antes → depois das otimizações):

| | Antes | Depois |
|---|---:|---:|
| **Media Start P95 — burst de 20 participantes** | **20 069 ms** | **873 ms** (~23×) |
| Heap — 20-participants | 253 MB | 53 MB |
| Heap — 10pub-10sub | 303 MB | 184 MB |
| `rtpbuffer` NACK retido (10pub-10sub, size 1024→256) | 116 MB | 62 MB |
| Goroutines P95 — burst | 545 | 175 |
| CPU profile — parsing de SDP na renegociação | ~24% | fora do top-16 |

O que resta como gargalo de CPU é o fan-out de RTP por subscription via
syscalls UDP, inerente à arquitetura — documentado como trabalho futuro em
`docs/perf/perf-optimization.md`.

**Autenticação não pesa no caminho de mídia**: com auth ligada e todo
participante carregando JWT, o profile de CPU do mesmo burst é idêntico —
nenhum frame de auth/HMAC no top; é custo de handshake (1 verificação HMAC por
conexão + 1 `bool` por operação), fora do loop de mídia.

### Testes de regressão automatizados

`internal/sfu/histogram_test.go`, `cmd/benchcheck/main_test.go`. Fuzzing dos
parsers de entrada não confiável (`FuzzParseToken`, `FuzzInbound`,
`FuzzClusterMessageValidate`) roda em CI a cada PR
(`.github/workflows/fuzz.yml`).

---

## Security & Authentication

A **fronteira de confiança** do PulseRTC. Sem isso, qualquer cliente que abrisse
um WebSocket usava o SFU; com isso, toda conexão carrega uma **identidade
autenticada** e toda operação sensível é checada contra **permissões
explícitas**.

### Como funciona

- **`internal/auth/`:** `claims.go` (Claims/Permissions/Identity),
  `token.go` (JWS compacto HMAC HS256/384/512, sem dependência externa),
  `validator.go` (assinatura → `exp`/`iat`/janela de replay/`sub`/`iss`/`aud`;
  proteção contra downgrade de `alg`), `permissions.go` (`AuthorizeJoin/Publish/
  Subscribe/Control`), `ratelimit.go` (token bucket por chave), `middleware.go`
  (`Authenticator`: extração de token, rate limit por IP, id de participante
  derivado do `sub`, proteção de endpoints HTTP), `metrics.go`, `config.go`
  (tudo por `PULSERTC_*`).
- **`internal/signaling`:** autenticação **antes** do upgrade em `HandleWS`
  (token inválido → `401`, socket nunca abre); `handleJoin` autoriza JOIN +
  claim `room`; `handleSFU` autoriza SUBSCRIBE; `handleControl` (CONTROL);
  timer de expiração de token com disconnect gracioso (`EXPIRED_TOKEN`); rate
  limit de mensagens por conexão; `PULSERTC_MAX_MESSAGE_SIZE`; erros de
  segurança padronizados com `code`.
- **`internal/sfu`:** `Permissions{Publish,Subscribe}` + `JoinWithPermissions`;
  `onTrack` recusa a Publication sem PUBLISH **antes** de alocar a track de
  fan-out; `Subscribe`/auto-subscribe recusam sem SUBSCRIBE.
- **Transporte do token (browser):** subprotocolo
  `pulsertc, pulsertc.token.<jwt>` (o WebSocket do navegador não seta headers).
  Não-browser: `Authorization: Bearer`. Fallback: `?access_token=`.
- **HTTP:** `/health` continua **público** (Docker health check); `/metrics`
  (Prometheus), `/metrics.json` (JSON legado), `/sfu/stats`, `/quality`,
  `/debug/pprof` exigem bearer (a menos de `PULSERTC_METRICS_PUBLIC=true`).
- **Observabilidade:** bloco `auth` em `/metrics`
  (`success/failed/tokenExpired/permissionDenied/roomDenied/rateLimited`);
  métricas genéricas `pulsertc_auth_*` derivadas do snapshot do subsistema.
  Token, header `Authorization` e segredo **nunca** aparecem em log.
- **Load test:** `--auth` — cada participante recebe um token próprio;
  `--auth-invalid-ratio` injeta uma fração de tokens inválidos; o runner manda
  bearer para `/metrics.json`.
- **Ferramenta:** `go run ./cmd/token -sub user-1 -room demo` gera um token.
- **Frontend:** campo **Token JWT** em `web/index.html`.
- **Modo dev:** `PULSERTC_AUTH_ENABLED=false` (default é `true` — falha fechado).

### Configuração (`.env.example`, `docker-compose.yml`)

| Variável | Default | Função |
|---|---|---|
| `PULSERTC_AUTH_ENABLED` | `true` | liga/desliga a autenticação |
| `PULSERTC_JWT_SECRET` | — | segredo HMAC (obrigatório se habilitado) |
| `PULSERTC_JWT_ISSUER` / `_AUDIENCE` | `pulsertc` | claims `iss` / `aud` esperados |
| `PULSERTC_JWT_ALGS` | `HS256` | algoritmos aceitos |
| `PULSERTC_TOKEN_MAX_AGE` | `1h` | janela de replay sobre `iat` |
| `PULSERTC_MAX_MESSAGE_SIZE` | `65536` | maior frame WebSocket aceito |
| `PULSERTC_RATE_LIMIT` | `120` | conexões + tentativas de auth por IP/min |
| `PULSERTC_MSG_RATE` | `100` | mensagens de signaling por conexão/s |
| `PULSERTC_METRICS_PUBLIC` | `false` | mantém `/metrics` etc. sem token |

Matriz de testes completa em `docs/security/authentication.md`.
Os parsers de token e do envelope WebSocket são fuzzados continuamente (ver
[Load testing & performance](#load-testing--performance)).

**Security ↑ · Performance ≈ · WebRTC = intacto.**

---

## Arquitetura distribuída

Base para **múltiplos nós** — do estado compartilhado até mídia cross-node,
load balancing e recuperação de falhas. Tudo opcional e aditivo:
`PULSERTC_CLUSTER_ENABLED=false` (default) mantém o comportamento single-node
byte-for-byte.

### Fundação: identidade de nó e ownership de room

- **`internal/cluster/` (interfaces + implementação in-memory):**
  - `node.go` — `NodeInfo` + ciclo de vida `STARTING→READY→ACTIVE→SHUTTING_DOWN→OFFLINE`; config `PULSERTC_NODE_*` / `PULSERTC_CLUSTER_*`.
  - `registry.go` — `NodeRegistry` (`InMemoryNodeRegistry`): register/unregister/heartbeat/get/list, idempotente, detecção de nó *stale* (a detecção de falha em si é discutida abaixo).
  - `room_locator.go` — `RoomLocator`: `ClaimOwner` **atômico** (1 mutex) → `(dono, claimeiEu)`. Garante "100 goroutines na mesma room → exatamente 1 dono".
  - `participant_locator.go` — `ParticipantLocator`: só a localização é compartilhada, nunca a sessão WebRTC.
  - `transport.go` + `http.go` — `NodeTransport` HTTP contra a API `/internal/*` do peer (timeout 3s, só no caminho de *join*, nunca no de mídia).
  - `internalauth.go` — credencial de cluster (`PULSERTC_CLUSTER_SECRET`), bearer, compare em tempo constante — **separada do JWT do usuário**. Chamada `/internal` sem credencial nunca é aceita.
  - `selector.go` — `NodeSelector` / `LocalNodeSelector` (room nova → este nó; room existente → dono atual, refinado pelo load balancing abaixo).
  - `cluster.go` — fachada `Cluster`: `ClaimRoom` (o coração do join), `ResolveRoom`, `RegisterParticipant`, heartbeat loop, `BeginShutdown`/`Shutdown`.
- **`internal/signaling`:** `handleJoin` chama `cluster.ClaimRoom` **antes** de criar qualquer estado local. Room de outro nó → `{"type":"error","code":"ROOM_ON_OTHER_NODE","nodeId":"…"}` e nenhum room/SFU peer local é criado. Registra a localização do participante no join, remove no disconnect; libera o ownership da room quando ela esvazia (sem migração).
- **HTTP:** `/ready` (readiness — 503 enquanto drenando); `/internal/node`, `/internal/nodes`, `/internal/rooms/{id}`, `/internal/participants/{id}` (404 se cluster desligado, 401 sem credencial). `/health` inalterado.
- **`cmd/server`:** marca o nó `READY` no boot, `BeginShutdown` no SIGTERM (para de aceitar rooms novas, sessões atuais terminam), `Shutdown` (para heartbeat, desregistra).
- **Observabilidade:** bloco `cluster` em `/metrics` (`nodes`/`nodesActive`/`nodesStale`, `rooms`/`roomsLocal`/`roomsRemote`, `ownershipClaimed`/`ownershipConflict`/`roomRedirects`, `internalRequests`/`…Failed`, `participantsLocated`). Logs estruturados; segredo de cluster nunca em log.

| Variável | Default | Função |
|---|---|---|
| `PULSERTC_CLUSTER_ENABLED` | `false` | liga a coordenação multi-nó |
| `PULSERTC_NODE_ID` | gerado | identidade do nó (definir explicitamente em cluster) |
| `PULSERTC_NODE_HOST` / `_PORT` | host / `PORT` | como os peers alcançam este nó |
| `PULSERTC_CLUSTER_SECRET` | — | credencial node-to-node (obrigatória se habilitado) |
| `PULSERTC_CLUSTER_PEERS` | — | URLs base dos outros nós (descoberta estática) |
| `PULSERTC_CLUSTER_HEARTBEAT` / `_STALE_AFTER` | `5s` / `15s` | cadência de heartbeat / idade para *stale* |

### Estado compartilhado (Redis)

Qualquer nó descobre `Room → Owner`, `Participant → Node` e `Node → Status` de
forma consistente. **Redis é coordenação, nunca mídia** — todo RTP/RTCP/SDP/ICE
continua local ao nó (`RTP → Local SFU → UDP`).

- `state.go` — `ClusterState` = `NodeRegistry` + `RoomLocator` +
  `ParticipantLocator` + `Backend` (health). Duas implementações atrás do
  mesmo contrato: `InMemoryClusterState` (dev/testes/CI/single-node) e
  `RedisClusterState`.
- `redis_client.go` — **único** componente que fala go-redis
  (`github.com/redis/go-redis/v9`): Connect/Ping/Close/Get/Set/SetNX/Del/
  Expire + `CompareDelete` (Lua server-side). Nada mais no código importa
  go-redis.
- `redis_state.go` — registry/room/participant sobre Redis. `ClaimOwner`
  = `SET key nodeId NX` (**atômico em todo o cluster**). Release =
  compare-and-delete. Namespace de chave configurável:
  `pulsertc:nodes:{id}`, `pulsertc:rooms:{id}`, `pulsertc:participants:{id}`.
- `cluster.go` — `New` escolhe o backend por `PULSERTC_REDIS_ENABLED`;
  `Start()` faz PING antes de ficar `READY`; heartbeat loop refaz o PING
  a cada tick, mede saúde, re-registra o nó (TTL) e loga
  `redis_connected`/`_disconnected`/`_reconnected`. `Ready()` = `false` quando
  Redis é obrigatório e está fora. `ClaimRoom` **fail-closed**
  (`CLUSTER_STATE_UNAVAILABLE`) quando não dá para determinar ownership.
  `ReleaseRoom` usa compare-and-delete.
- **Node TTL + heartbeat:** chave de nó expira em `NODE_TTL` (15s);
  heartbeat (5s) renova. Sem heartbeat → chave some → nó *stale*.
- **Participant location TTL:** chave expira em `PARTICIPANT_TTL` (60s); a
  sessão local WebRTC continua sendo a fonte da verdade da conexão.
- **Lifecycle:** `/ready` = processo operacional (`cluster state available`
  **e** `redis available`); `/health` continua respondendo mesmo com Redis
  fora.
- **Docker:** `deploy/docker-compose.redis.yml` — `redis:7-alpine` +
  `node-01` + `node-02`. Redis numa rede `internal: true` **sem portas
  publicadas** — só os nós alcançam. Persistência desligada: coordenação
  efêmera; após restart os nós reconectam e re-registram.

| Variável | Default | Função |
|---|---|---|
| `PULSERTC_REDIS_ENABLED` | `false` | usa Redis como backend de estado compartilhado |
| `PULSERTC_REDIS_ADDR` | `redis:6379` | endereço do Redis |
| `PULSERTC_REDIS_PASSWORD` | — | auth (injetar; nunca hardcode) |
| `PULSERTC_REDIS_DB` | `0` | número do DB |
| `PULSERTC_REDIS_PREFIX` | `pulsertc` | namespace de chave |
| `PULSERTC_REDIS_REQUIRED` | `true` | Redis é dependência dura; `false` = start degradado |
| `PULSERTC_REDIS_TIMEOUT` | `3s` | timeout de dial/comando |
| `PULSERTC_REDIS_NODE_TTL` | `15s` | TTL da chave de nó |
| `PULSERTC_REDIS_PARTICIPANT_TTL` | `60s` | TTL da localização de participante |
| `PULSERTC_REDIS_ROOM_TTL` | `0` | TTL do ownership de room; `0` = release explícito |

**Benchmarks** (miniredis loopback — real Redis soma o RTT de rede):

| Operação | Redis | Local (in-memory) |
|---|---|---|
| `ClaimRoom` (SETNX) | ~119 µs / 32 allocs | 48 ns / 0 alloc |
| `ResolveRoom` hit (GET) | ~121 µs / 27 allocs | 19 ns (miss) |
| `ReleaseRoom` (claim+Lua CAD) | ~348 µs | — |
| `RegisterParticipant` (SET EX) | ~123 µs | — |
| `ResolveParticipant` (GET) | ~113 µs | — |
| `RegisterNode` / `Heartbeat` | ~216 / ~221 µs | — |

O objetivo não é competir com memória — é conhecer o custo da consistência
distribuída, que fica **só no join / lookup**, nunca no forwarding de RTP.

### Sinalização cross-node

Uma mensagem cujo participante-alvo está em outro nó é roteada para o nó
certo, autenticada e idempotente, **sem transportar mídia**. RTP/RTCP/SRTP/
áudio/vídeo continuam 100% locais ao SFU.

- `message.go` — `ClusterMessage` (envelope node→node: `type`, `requestId`,
  `sourceNodeId`, `targetNodeId`, `timestamp`, `roomId`, `participantId`,
  `payload`) + `Validate` (tipo/campos/idade/tamanho). Tipos:
  `participant.message`, `participant.disconnect`, `participant.control`,
  `room.event` (+ 6 reservados).
- `messagetransport.go` — `MessageTransport` (**separado** do `NodeTransport`
  e do Redis): `InMemoryMessageTransport` (testes, multi-nó no mesmo
  processo) + `HTTPMessageTransport` (`POST /internal/cluster/messages`).
  Resolve endereço via `NodeRegistry` (só nós `READY`), timeout, retry
  limitado com backoff, limite de payload, métricas de latência.
- `router.go` — `ParticipantRouter`: cache pequeno de localização (TTL 2s,
  **nunca autoridade**) → `ParticipantLocator` → decide **local** (entrega
  direta, sem rede) / **remoto** (`MessageTransport`, um nó só) /
  **inexistente** (`PARTICIPANT_NOT_FOUND`). Stale location: re-resolve uma
  vez pela fonte autoritativa. Redis fora → fail-closed
  `CLUSTER_STATE_UNAVAILABLE`.
- `messagehandler.go` — `ClusterMessageHandler`: valida envelope → checa
  `targetNodeId` (loop prevention, `CLUSTER_TARGET_MISMATCH`) → dedup por
  `requestId` → despacha ao componente local. HTTP não contém lógica de
  processamento.
- `dedup.go` — `dedupCache` local com TTL + sweep; Redis **não** é usado por
  mensagem.
- `http.go` — `POST /internal/cluster/messages` (guard: cluster habilitado +
  `InternalAuthMiddleware` + `MaxBytesReader` → 413).
- `cluster.go` — `SetLocalDelivery`, `Router()`, `RouteMessage`,
  `BroadcastRoomEvent` (fan-out para nós remotos com participantes da sala);
  wiring de lifecycle (dedup goroutine em `Start`, `dedup.Close` +
  `msgTransport.Close` em `Shutdown`).
- **`internal/signaling/`:** `Server` implementa `cluster.LocalDelivery`
  (`DeliverToParticipant` → socket local / `disconnect`; `DeliverRoomEvent` →
  broadcast local), com um índice `clients map[string]*Client`.
  `handleForward` — quando o alvo não está na sala local **e** o cluster está
  ligado: valida via `LocateParticipant` que o alvo está na **mesma sala** em
  outro nó, então `cluster.RouteMessage`; erro `*cluster.Error` volta ao
  cliente com o código. Cross-room continua impossível.
  `participant_joined` / `participant_left` também são propagados aos membros
  da sala em outros nós.
- **Erros/HTTP:** `CLUSTER_NODE_NOT_FOUND` (404), `CLUSTER_NODE_UNAVAILABLE`
  (503), `CLUSTER_MESSAGE_INVALID` (400), `CLUSTER_MESSAGE_TOO_LARGE` (413),
  `CLUSTER_MESSAGE_EXPIRED` (400), `CLUSTER_TARGET_MISMATCH` (409),
  `PARTICIPANT_NOT_FOUND` (404). Corpo só `{"error":"<CODE>"}`.
- **Observabilidade:** `/metrics.cluster` ganha `messages.{sent,received,
  failed,retried,duplicated,rejected,invalid,timeout}`, `routing.{local,remote,
  notFound}`, `transportErrors`, `transportLatencyMsAvg` (labels de baixa
  cardinalidade). Logs `cluster_message_{received,failed,rejected}`,
  `cross_node_{forward_failed,disconnect}`; nunca loga secret/JWT/Authorization.

```
PULSERTC_CLUSTER_REQUEST_TIMEOUT=2s
PULSERTC_CLUSTER_MAX_RETRIES=2
PULSERTC_CLUSTER_MAX_MESSAGE_SIZE=65536
PULSERTC_CLUSTER_MESSAGE_MAX_AGE=30s
```

**Benchmarks:**

| Bench | ns/op | allocs/op |
|---|---|---|
| `LocalRoute` (inclui `NewClusterMessage`) | ~1050 | 2 |
| `RemoteRoute` (in-mem transport) | ~1560 | 4 |
| `ClusterMessageValidation` | ~56 | 0 |
| `MessageDeduplication` | ~82 | 0 |

O caminho local não constrói o transporte e não toca a rede. Profiling do SFU
durante forwarding RTP: `MessageTransport` / `ParticipantRouter` /
`ClusterMessageHandler` / HTTP cluster **não aparecem** — arquitetura de RTP
inalterada.

### Mídia cross-node (SFU ↔ SFU)

Participantes da mesma sala em nós diferentes trocam áudio/vídeo. Cada SFU
continua fazendo forwarding local; um transporte **UDP dedicado** carrega só o
RTP/RTCP que precisa cruzar a fronteira do nó. **Redis e o HTTP de sinalização
nunca carregam mídia** — provado por teste de integração com dois SFUs Pion.

- `mediatransport.go` — `MediaTransport` + `udpMediaTransport`: um socket
  UDP por nó, datagramas com frame de 21 bytes (`magic|version|kind|sessionID|len`),
  RTP/RTCP multiplexados por 1 byte de tipo, auth por HELLO
  HMAC-SHA256(`PULSERTC_CLUSTER_SECRET`, `sessionID‖src‖dst`) — sessão só
  `ACTIVE` após o HELLO round-trip; datagrama sem sessão conhecida do endereço
  confirmado é descartado e contado, nunca logado por pacote.
- `mediaconn.go` — `MediaConnection`: fila de envio limitada por sessão
  (backpressure — peer lento derruba só os próprios pacotes), heartbeat,
  detecção de link quebrado.
- `mediasession.go` — `MediaSession` (`CREATING→ACTIVE→CLOSED`), uma por
  par de nós reusada para todas as tracks; `Close` idempotente.
- `remotetrack.go` — `RemoteTrack` (`CREATING→ACTIVE→MUTED→ENDED`) +
  `RemotePublicationInfo` + interfaces `MediaSink`/`MediaSource` (visão do SFU).
- `mediabridge.go` — orquestrador: handshake sobre o `MessageTransport`
  (`media.session.open/accept`, `media.subscribe(.ack)`,
  `media.unsubscribe`, `media.track.ended`), `SubscribeRemote`,
  `PublicationEnded`, demux de RTP/RTCP de entrada. Nó desaparece → sessão
  perdida (heartbeat/timeout) → RemoteTracks encerradas → subscribers
  notificados.
- `cluster.go` — `Media() MediaBridge` (no-op quando desabilitado); `New`
  constrói o bridge quando `PULSERTC_MEDIA_ENABLED`; `Shutdown` fecha o
  transporte; `NodeInfo.MediaAddr` propagado no registry para descoberta.
- `metrics.go` — bloco `cluster.media.*` (sessions, rtp/rtcp sent/received/
  dropped/invalid, oversizedPackets, authFailures — sem labels de alta
  cardinalidade).
- **`internal/sfu/` — sink/source:** `remote.go` (`SFU` implementa
  `cluster.MediaSource`: `AttachForwarder` liga a fan-out da publicação local
  a uma sessão; `DeliverRTCPToPublisher` traduz PLI/FIR do subscriber remoto
  para o publisher local; `cluster.MediaSink` via `AddRemotePublication`).
  `room.go` cria uma `Publication` sintética roteada pelo caminho normal de
  `onPublicationAdded`. Forwarding puro, sem transcode (Pion reescreve
  SSRC/PT por binding). `publication.go` — `remoteSinks` copy-on-write
  (leitura sem lock no hot path). `participant.go` — PLI/FIR de uma pub
  remota volta ao nó de origem em vez de a um publisher local inexistente.
- **`internal/signaling/`:** fia sink/source/hooks; índice `room|pub → nó`;
  `publication_added/removed` fan-out como `room.event` cross-node; ao
  `subscribe` de uma pub não-local mas conhecida, `ensureRemoteMirror`
  estabelece o espelho; teardown no último unsubscribe, no fim da pub, e
  quando a sala fica sem participantes locais.
- **Erros:** `MEDIA_SESSION_{NOT_FOUND,INVALID,EXPIRED}`,
  `MEDIA_{NODE_UNAVAILABLE,AUTH_FAILED,PACKET_INVALID,PACKET_TOO_LARGE,
  TRANSPORT_TIMEOUT,TRANSPORT_CLOSED}`, `REMOTE_TRACK_NOT_FOUND`.
- **Docs:** `docs/networking/cross-node-media.md`, ADR 006/007.

```
PULSERTC_MEDIA_ENABLED=false
PULSERTC_MEDIA_BIND_ADDR / _PORT / _ADVERTISE_HOST
PULSERTC_MEDIA_REQUEST_TIMEOUT / _MAX_PACKET_SIZE / _SESSION_TIMEOUT
PULSERTC_MEDIA_HEARTBEAT_INTERVAL / _SEND_QUEUE
```

Testes: dois SFUs reais + `PeerConnection`s Pion — Alice(node-a) publica vídeo
→ Bob(node-b) recebe pelo UDP MediaTransport; bidirecional (Alice áudio ⇄ Bob
vídeo); unpublish encerra a publicação espelhada; e um teste de morte
**silenciosa** de nó (socket fechado sem BYE) provando que a sessão é
reconhecida como perdida via `SessionTimeout`/heartbeat, não só na saída
graciosa. **Benchmarks:** `RTPForward` ~1-2 µs/op (1 cópia de framing + 1
`sendto`, sem lock no hot path), `PacketValidation` 0 alloc.

### Load balancing / Room routing

Novas salas são criadas no **nó menos carregado** disponível, em vez de
sempre no nó onde o primeiro cliente se conectou. O scheduler está **só** no
caminho de criação de sala — nunca no caminho de RTP. Salas existentes nunca
são re-roteadas; `ClaimOwner` continua a autoridade do ownership.

- `roomrouter.go` — `RoomRouter` (`SelectNode`) + `RoomSelectionPolicy`
  (interface) + `LeastLoadedPolicy`: score = `participants*1 +
  publications*0.5 + subscriptions*0.25 + rooms*2`, desempate
  **determinístico por nodeId**, *prefer-local margin* para preservar
  comportamento single-node e evitar redirects desnecessários. Filtra
  nós não-`READY` / stale / `DRAINING` / `OFFLINE`; exclui nós
  acima do hard limit de capacidade → `NO_CAPACITY`. Correção
  local *pending* espalha rajadas dentro de um intervalo de report.
- `load.go` — `NodeLoad`, `LoadStore` (+ `InMemoryLoadStore`),
  `loadReporter` (agrega contadores locais e publica no estado compartilhado a
  cada `PULSERTC_LOAD_REPORT_INTERVAL`, TTL = 3× intervalo — **nunca por
  pacote RTP**). Report stale (> 5× intervalo) é ignorado.
  `PutRoomAssignment`/`RoomAssignment` — hint de curto TTL para o redirect não
  ficar em ping-pong.
- `redis_state.go` — `redisLoadStore` (`pulsertc:load:<id>` +
  `pulsertc:assign:<room>`).
- `cluster.go` — `ClaimRoom` consulta o `RoomRouter` **só para salas novas**;
  candidato remoto → `ROOM_ON_OTHER_NODE{node}`; candidato local →
  `ClaimOwner` atômico. `SetLoadProvider`, `RoomRouter()`, `LoadSnapshot()`.
  Reporter parado + `DeleteLoad` no shutdown.
- `http.go` — `GET /internal/cluster/load` (snapshot para debug).
- `metrics.go` — blocos `cluster.scheduler.*` (roomSelections, failures,
  roomClaims, retries, selectLocal/LeastLoaded/Capacity/Fallback) e
  `cluster.load.*` (reports, stale). Reason como label controlado.
- **`internal/sfu/`** — `SFU.LoadCounts()` (rooms/participants/publications/
  subscriptions ativos, exclui pubs remotas espelhadas).
- **`internal/signaling/`** — `wireLoadProvider`: fornece ao cluster os
  contadores vivos + heap/goroutines uma vez por intervalo.
- **Docs:** `docs/perf/load-balancing.md`, ADR 008/009.

```
PULSERTC_LOAD_REPORT_INTERVAL=2s
PULSERTC_NODE_MAX_{ROOMS,PARTICIPANTS,PUBLICATIONS}=0   # 0 = sem limite
PULSERTC_NODE_{SOFT,HARD}_LIMIT=0.8 / 1.0
```

Testes: policy (vazio/1/N nós, carga igual/diferente, tie-break, prefer-local
margin, ponderação); router (draining/offline/stale excluídos, hard limit +
`NO_CAPACITY`, fail-closed com estado indisponível, **distribuição de 300
salas / 3 nós dentro de 20%**, 100 seleções concorrentes); cluster real — nova
sala roteada ao menos carregado, **sala existente nunca re-roteada**, **join
storm de 120 salas concorrentes → owner único + spread entre 3 nós**, todos os
nós DRAINING → erro.

### Detecção de falha e recuperação de sessão

Quando o heartbeat de um nó expira, o cluster o marca `OFFLINE` e permite que
outro nó assuma suas rooms — para que **novos** participantes continuem
entrando. As sessões WebRTC já estabelecidas no nó morto são perdidas: não há
migração de `PeerConnection`; o cliente reconecta (para onde a room agora
resolve), monta uma `RTCPeerConnection` nova e reentra carregando seu id de
sessão + geração + intenção de publish/subscribe.

- **Failure detector** — varre os nós conhecidos num intervalo fixo (nunca por
  pacote), com um período de graça extra antes de declarar `OFFLINE`
  (absorve uma pausa de GC ou uma falha breve de rede/Redis). Nunca marca o
  próprio nó como stale.
- **Recovery manager** — ao detectar um nó stale, transfere o ownership das
  suas rooms para o nó local (bump de geração, uma única vez, mesmo sob
  corrida concorrente) e limpa os registros de localização de participante
  que apontavam para o nó morto. Pula nós saudáveis. Fail-closed: sem backend
  de estado disponível, nada é recuperado.
- **Recuperação de sessão** — o cliente carrega `sessionId` + `generation` no
  reconnect; o servidor valida a geração, faz o fencing de sessões obsoletas e
  restaura a intenção de publish/subscribe declarada antes da queda. Backoff
  de reconexão (inicial/máximo/jitter) é comunicado ao cliente no
  `room_joined` para toda a frota reconectar com a mesma disciplina.
- **Mídia cross-node sobrevive à morte do nó de origem** — coberta na seção
  anterior (o `SessionTimeout` do transporte UDP detecta a morte silenciosa e
  encerra o `RemoteTrack` espelhado).

```
PULSERTC_FAILURE_CHECK_INTERVAL=5s
PULSERTC_FAILURE_GRACE_PERIOD=5s
PULSERTC_RECOVERY_CONCURRENCY=4
PULSERTC_RECOVERY_ENABLED=true
PULSERTC_SESSION_RECOVERY_ENABLED=true
PULSERTC_SESSION_RECOVERY_TIMEOUT=30s
PULSERTC_RECONNECT_INITIAL_DELAY=500ms / _MAX_DELAY=10s / _JITTER=20%
```

Testes: três nós reais compartilhando estado — um deles "crasha" (heartbeat
parado, deixando um registro que expira por TTL em vez de um shutdown
gracioso), o failure detector real de um peer nota, a room é reclamada por um
nó vivo com a geração avançando exatamente uma vez, o registro de participante
do nó morto deixa de resolver, e a room saudável de outro nó permanece
intocada.

### Fora de escopo (por ora)

Simulcast/SVC, gravação, transcoding, egress/ingress; TURN embarcado por
padrão (ver `docs/deploy.md` para configurar coturn);
migração de room / de `PeerConnection` entre nós; NACK/FEC ou congestion
control cross-node distribuído; rebalanceamento ao vivo de carga; autoscaling
de infraestrutura; geo/latency routing; mTLS obrigatório entre nós;
multi-tenancy (um deployment serve um dono).

---

## Integration API (`/v1`)

Camada HTTP pública para uma aplicação externa (o backend de negócio)
controlar o PulseRTC sem conhecer SFU/cluster/Pion. **Guia público completo
(HTTP + WebSocket) para sistemas externos:**
docs/api/integration-guide.md · guia REST:
docs/api/api-integration.md · contrato:
[docs/api/openapi.yaml](docs/api/openapi.yaml) · decisões:
docs/adr/010-integration-api.md.

- **Control plane apenas** — a API nunca entra no caminho de RTP. O `join`, a
  negociação WebRTC e o Session Recovery continuam no WebSocket `/ws`.
- **Autenticação própria**: `PULSERTC_API_KEYS` (`key:scope,scope;key2:*`),
  escopos `rooms:read/create/close`, `tokens:create`, `participants:read`,
  `quality:read` — separados das permissões do participante e do cluster secret.
  Nunca chega ao browser.
- **Endpoints**: `POST/GET/DELETE /v1/rooms/{id}`, `POST .../tokens` (minta um
  JWT via `auth.Sign`, aceito pelo `/ws`), `GET .../connection`,
  `.../participants[/{id}[/session]]`, `.../quality`,
  `.../participants/{id}/quality`. Room de outro node → `409 ROOM_ON_OTHER_NODE`.
- **Webhooks** mínimos (HMAC), opcionais: `room.created/closed`,
  `participant.joined/left`, ligados por `PULSERTC_WEBHOOK_URL`.
- `internal/api` (handlers → service → `Core`), adapter `signaling.NewAPICore`,
  `Server.CloseRoom` (envia `room_closed` e fecha os sockets). Variáveis:
  `PULSERTC_API_*`, `PULSERTC_PUBLIC_WS_URL`, `PULSERTC_WEBHOOK_*` (ver
  `.env.example`).
- Testes: unit + handlers (`internal/api`), integração e **E2E multi-node**
  (`internal/signaling/api_*_test.go`).

---

## Problemas encontrados

- **Glare com dynamic publish.** A regra original (só o SFU oferece após a
  1ª offer) quebra quando o browser adiciona uma track depois. Resolvido com
  **perfect negotiation** (SFU impolite, browser polite + rollback).
- **Publisher não conhecia o id da própria publication.** O SFU só notificava os
  *outros*. Passou a enviar `publication_added` também para o próprio publisher.
- **`Publication` como conjunto vs uma track.** Refatorado para uma
  `Publication` por track; o tipo antigo (`PublishedTrack`) foi removido.
- **Pion v4.2 exige Go 1.24.** O projeto subiu de 1.23 → 1.24 (go.mod +
  Dockerfile `golang:1.24-alpine`).
- **ICE atrás do Docker.** As host candidates do container têm IP privado
  (172.x) inalcançável pelo browser no host. Resolvido com UDP mux em porta
  única publicada (`SFU_UDP_PORT`) + `SetNAT1To1IPs(SFU_NAT_1TO1_IP)`.
- **Glare de renegociação.** Evitado pelo contrato "browser faz a 1ª offer,
  depois só o SFU é offerer" + flag `renegotiate` que reenfileira uma
  renegociação pendente quando o estado volta a `stable`.
- **ICE candidate antes da remote description** (trickle chega antes do
  answer/offer). Bufferizado dos dois lados e drenado após `setRemoteDescription`.
- **`go test -race` exige CGO.** A imagem `golang:*-alpine` não traz toolchain C;
  o alvo de race usa `golang:1.24` (Debian) com `CGO_ENABLED=1`.
- **Escritas concorrentes no WebSocket** do Gorilla causam panic. Resolvido com
  a goroutine única de escrita + canal `send`.
- **Fechar o canal `send` duas vezes** (backpressure + disconnect). Resolvido com
  a flag `closed` sob mutex do client.
- **`CheckOrigin`** default do Gorilla bloqueia cross-origin; liberado
  (sem verificação) e documentado como algo a revisitar antes de abrir para
  público geral.

## Melhorias futuras identificadas

- Simulcast / camadas de qualidade adaptativa; controle de bitrate.
- TURN embarcado habilitado por padrão (hoje é opt-in via `docker compose --profile turn`).
- Migração de room / `PeerConnection` entre nós (hoje a recuperação reclama a
  room, mas a sessão de mídia em si reconecta do zero).
- Multi-tenancy (namespace de rooms e cotas por cliente da Integration API).
- Broadcast de `signal` (sem `target`) para toda a room, se algum caso de uso pedir.
- Baseline de load test medido em hardware real (o baseline atual em
  `benchmarks/baseline/` foi semeado num container onde ICE/DTLS não
  estabelece de verdade).
