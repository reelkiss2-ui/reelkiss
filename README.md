# Tingle Socket.IO Server

Real-time server for Tingle live streaming app.

## Iniciar

```bash
# Instalar dependências
npm install

# Produção
npm start

# Desenvolvimento (com auto-reload)
npm run dev
```

O servidor corre na porta **3000** por padrão.
Para mudar: `PORT=4000 npm start`

---

## Configurar no Flutter

No ficheiro `TINGO/lib/utils/api.dart`, altere:

```dart
static const baseUrl = "";          // antes (vazio = sem socket)
static const baseUrl = "http://SEU_IP:3000";   // depois
```

**Exemplos de URL:**
- Rede local:  `http://192.168.1.100:3000`
- Produção:    `https://socket.seudominio.com`

---

## Configurar no Admin Panel

Em **Settings → Socket Server Setting**, coloque o mesmo URL.
Isso permite ao admin encerrar lives remotamente.

---

## Eventos suportados

| Categoria    | Eventos                                                                 |
|--------------|-------------------------------------------------------------------------|
| Live Room    | joinLiveRoom, countLiveJoin, reduceLiveJoiners, broadcastLiveComment    |
| Gifts        | giftToLiveStream → fetchLiveGifts, liveRoomCoinUpdate, topLiveStreamGifter |
| PK Battle    | pkRequest → pkRequestReceived, pkAnswer, pkGift (com scores), pkEnd     |
| Audio Room   | hostJoinAudioRoom, participantAdded, seatUpdate, giftInAudioRoom        |
| Calls        | initiateCall → incomingCall, handleCallResponse, callEnded              |
| Messages     | messageSent (1:1 chat)                                                  |
| Admin        | endLiveStream (encerra a live no dispositivo)                           |

---

## Deploy em produção (opções gratuitas)

### Railway.app (recomendado)
1. Criar conta em railway.app
2. New Project → Deploy from GitHub
3. Selecionar esta pasta
4. URL gerada automaticamente (ex: `https://tingle-socket.up.railway.app`)

### Render.com
1. New Web Service → Connect repo
2. Build: `npm install`, Start: `npm start`
3. URL gerada automaticamente

### VPS próprio (Node.js + PM2)
```bash
npm install -g pm2
pm2 start index.js --name tingle-socket
pm2 startup
pm2 save
```

---

## Health check

```
GET http://localhost:3000/health
→ { "status": "ok", "connections": 5, "rooms": 2 }
```
