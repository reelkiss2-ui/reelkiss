/**
 * Tingle Socket.IO Server
 *
 * Handles all real-time events for the Tingle app:
 *  • Live streaming  — comments, gifts, viewer count, entry effects
 *  • PK Battle       — request, accept/reject, gifts, rank, end
 *  • Audio Room      — seats, reactions, gifts
 *  • Video Calls     — initiate, accept, reject, end
 *  • Chat Messages   — 1:1 messaging
 *  • Admin actions   — end stream remotely
 *
 * Flutter connects via:
 *   socket_io_client with query: { "globalRoom": "globalRoom:{userId}" }
 */

const { createServer } = require('http')
const { Server }       = require('socket.io')

// ── Firebase Admin (optional, for Firestore live data) ─────────────────────────
let db = null
try {
  const admin = require('firebase-admin')
  if (!admin.apps.length) {
    admin.initializeApp({ projectId: process.env.FIREBASE_PROJECT_ID || 'reelkiss-97fa4' })
  }
  db = admin.firestore()
} catch (e) {
  console.warn('Firebase Admin not available:', e.message)
}

const PORT = process.env.PORT || 3333

const httpServer = createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ status: 'ok', connections: userSockets.size, rooms: liveRooms.size }))
  } else {
    res.writeHead(404); res.end()
  }
})

const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket', 'polling'],
  pingTimeout: 60000,
  pingInterval: 25000,
})

// ─── State ─────────────────────────────────────────────────────────────────────
/** userId → Socket */
const userSockets = new Map()

/** liveHistoryId → Set<socketId> */
const liveRooms = new Map()

/** liveHistoryId → Map<userId, { userId, name, image, userName, isProfilePicBanned }> */
const liveViewers = new Map()

/** liveHistoryId → { host1UserId, host2UserId, score1, score2, startedAt } */
const pkBattles = new Map()

// ─── Helpers ───────────────────────────────────────────────────────────────────

function parse(raw) {
  if (typeof raw === 'string') { try { return JSON.parse(raw) } catch { return {} } }
  return raw ?? {}
}

function getUserId(socket) {
  const q = socket.handshake.query.globalRoom || ''
  return q.replace('globalRoom:', '').trim()
}

function socketOf(userId) { return userSockets.get(userId) }

function emit(userId, event, data) {
  const s = socketOf(userId)
  if (s) { s.emit(event, data); return true }
  return false
}

function addToRoom(liveHistoryId, socketId) {
  if (!liveRooms.has(liveHistoryId)) liveRooms.set(liveHistoryId, new Set())
  liveRooms.get(liveHistoryId).add(socketId)
}

function addViewer(liveHistoryId, userId, info) {
  if (!userId) return
  if (!liveViewers.has(liveHistoryId)) liveViewers.set(liveHistoryId, new Map())
  liveViewers.get(liveHistoryId).set(userId, {
    userId,
    name:               info.name               ?? '',
    image:              info.image               ?? '',
    userName:           info.userName            ?? '',
    // Flutter sends "senderProfilePicBanned"; normalise to "isProfilePicBanned"
    isProfilePicBanned: info.isProfilePicBanned ?? info.senderProfilePicBanned ?? false,
    avtarFrame:         info.avtarFrame          ?? '',
    avtarFrameType:     info.avtarFrameType      ?? 0,
    isFollow:           false,
  })
}

function removeViewer(liveHistoryId, userId) {
  liveViewers.get(liveHistoryId)?.delete(userId)
}

function getViewerList(liveHistoryId) {
  return Array.from(liveViewers.get(liveHistoryId)?.values() ?? [])
}

function removeFromAllRooms(socketId, userId) {
  for (const [id, sockets] of liveRooms) {
    sockets.delete(socketId)
    if (sockets.size === 0) liveRooms.delete(id)
  }
  if (userId) {
    for (const [id, viewers] of liveViewers) {
      viewers.delete(userId)
      if (viewers.size === 0) liveViewers.delete(id)
    }
  }
}

function broadcastToRoom(liveHistoryId, event, data, excludeSocketId = null) {
  const room = liveRooms.get(liveHistoryId)
  if (!room) return 0
  let count = 0
  for (const sid of room) {
    if (sid === excludeSocketId) continue
    const s = io.sockets.sockets.get(sid)
    if (s) { s.emit(event, data); count++ }
  }
  return count
}

function viewerCount(liveHistoryId) {
  return liveRooms.get(liveHistoryId)?.size ?? 0
}

function log(tag, msg) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] [${tag}] ${msg}`)
}

// ─── Connection ────────────────────────────────────────────────────────────────

io.on('connection', (socket) => {
  const userId = getUserId(socket)
  if (userId) userSockets.set(userId, socket)
  log('CONNECT', `user=${userId} socket=${socket.id}`)

  // ── LIVE ROOM ────────────────────────────────────────────────────────────────

  // VIEWER requests single live object data (to initialize LiveController)
  socket.on('fetchSingleLiveUser', async (raw) => {
    const { liveHistoryId, liveUserObjId, userId: requesterId } = parse(raw)
    if (!liveHistoryId) return
    log('fetchSingleLiveUser', `requester=${requesterId} room=${liveHistoryId}`)

    if (db) {
      try {
        // Try liveUserObjId first, then liveHistoryId
        const docId = liveUserObjId || liveHistoryId
        const doc = await db.collection('liveStreams').doc(docId).get()
        if (doc.exists) {
          socket.emit('singleLiveUserResponse', { _id: doc.id, ...doc.data() })
          return
        }
      } catch (e) {
        console.warn('fetchSingleLiveUser Firestore error:', e.message)
      }
    }

    // Fallback: ask the host to re-broadcast live data
    const hostUserId = liveRooms.has(liveHistoryId)
      ? [...liveRooms.get(liveHistoryId)].map(sid => {
          for (const [uid, s] of userSockets) { if (s.id === sid) return uid }
          return null
        }).find(Boolean)
      : null

    if (hostUserId) {
      const hostSock = userSockets.get(hostUserId)
      if (hostSock) hostSock.emit('requestLiveObjectBroadcast', { liveHistoryId, requesterId })
    } else {
      // No host found — live may have ended
      socket.emit('singleLiveUserResponse', null)
    }
  })

  // HOST emits this when they start their own live room
  socket.on('joinLiveRoom', (raw) => {
    const { liveHistoryId } = parse(raw)
    if (!liveHistoryId) return
    addToRoom(liveHistoryId, socket.id)
    log('joinLiveRoom', `user=${userId} room=${liveHistoryId} viewers=${viewerCount(liveHistoryId)}`)
  })

  // VIEWERS emit this when they enter a live stream
  socket.on('countLiveJoin', (raw) => {
    const data = parse(raw)
    const { liveHistoryId, userId: viewerUserId } = data
    if (!liveHistoryId) return
    addToRoom(liveHistoryId, socket.id)
    addViewer(liveHistoryId, viewerUserId || userId, data)
    const count = viewerCount(liveHistoryId)
    log('countLiveJoin', `user=${userId} room=${liveHistoryId} viewers=${count}`)
    // Notify room of new viewer (send as JSON string for Flutter compatibility)
    broadcastToRoom(liveHistoryId, 'countLiveJoin', JSON.stringify({ ...data, viewerCount: count }))
    // Send entry effect if provided
    if (data.entryRide) broadcastToRoom(liveHistoryId, 'fireEntryEffect', data, socket.id)
    // Broadcast full viewer list so the drawer updates
    broadcastToRoom(liveHistoryId, 'liveViewersList', getViewerList(liveHistoryId))
  })

  // VIEWERS emit this when they leave a live stream
  socket.on('reduceLiveJoiners', (raw) => {
    const data = parse(raw)
    const { liveHistoryId, userId: viewerUserId } = data
    if (!liveHistoryId) return
    liveRooms.get(liveHistoryId)?.delete(socket.id)
    removeViewer(liveHistoryId, viewerUserId || userId)
    const count = viewerCount(liveHistoryId)
    broadcastToRoom(liveHistoryId, 'reduceLiveJoiners', JSON.stringify({ ...data, viewerCount: count }))
    broadcastToRoom(liveHistoryId, 'liveViewersList', getViewerList(liveHistoryId))
    log('reduceLiveJoiners', `user=${userId} room=${liveHistoryId} viewers=${count}`)
  })

  // Comments in a live stream
  socket.on('broadcastLiveComment', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'broadcastLiveComment', raw, socket.id)
  })

  // Gift sent to a live stream
  socket.on('giftToLiveStream', (raw) => {
    const data = parse(raw)
    const { liveHistoryId, receiverUserId } = data
    if (!liveHistoryId) return
    // Broadcast gift animation to all viewers
    broadcastToRoom(liveHistoryId, 'fetchLiveGifts', data)
    // Update coin total for host
    broadcastToRoom(liveHistoryId, 'liveRoomCoinUpdate', data)
    // Update top gifters
    broadcastToRoom(liveHistoryId, 'topLiveStreamGifter', data)
    log('giftToLiveStream', `room=${liveHistoryId} receiver=${receiverUserId}`)
  })

  // End live stream (by host or admin)
  socket.on('endLiveStream', (raw) => {
    const data = parse(raw)
    const { liveHistoryId, userId: hostId } = data
    if (!liveHistoryId) return
    log('endLiveStream', `room=${liveHistoryId} by=${userId}`)
    broadcastToRoom(liveHistoryId, 'endLiveStream', data)
    liveRooms.delete(liveHistoryId)
    pkBattles.delete(liveHistoryId)
  })

  // Entry ride / effect when viewer enters
  socket.on('fireEntryEffect', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'fireEntryEffect', data, socket.id)
  })

  // Reactions (emoji effects)
  socket.on('broadcastReaction', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'broadcastReaction', raw, socket.id)
  })

  // Background theme change
  socket.on('updateLiveTheme', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'themeUpdated', data)
  })

  // Block a user in the live room
  socket.on('addToBlockedList', (raw) => {
    const data = parse(raw)
    const { liveHistoryId, blockedUserId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'addToBlockedList', data)
    emit(blockedUserId, 'notifyBlockedUser', data)
  })

  socket.on('removeFromBlockedList', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'removeFromBlockedList', data)
  })

  // Resume broadcast (reconnect after drop)
  socket.on('resumeLiveBroadcast', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    addToRoom(liveHistoryId, socket.id)
    socket.emit('resumeLiveBroadcast', data)
  })

  // ── PK BATTLE ────────────────────────────────────────────────────────────────

  // HOST_1 sends a PK challenge to HOST_2
  socket.on('pkRequest', (raw) => {
    const data  = parse(raw)
    const host2 = data.host2Id
    if (!host2) return
    log('pkRequest', `from=${userId} to=${host2}`)
    emit(host2, 'pkRequestReceived', raw)
  })

  // HOST_2 accepts or rejects the PK challenge
  socket.on('pkAnswer', (raw) => {
    const data      = parse(raw)
    const host1Id   = data.HOST_1_ID   // answerer
    const host2Id   = data.HOST_2_ID   // requester (original challenger)
    const isAccept  = data.isAccept
    const host1Live = data.HOST_1_LIVEID
    const host2Live = data.HOST_2_LIVEID

    log('pkAnswer', `from=${userId} to=${host2Id} accept=${isAccept}`)

    const PK_DURATION = 300 // 5 minutes in seconds
    const response = { data: raw, response: { duration: PK_DURATION } }

    // Send pkAnswer to the two hosts directly
    const host1Sock = socketOf(host1Id)
    const host2Sock = socketOf(host2Id)
    if (host1Sock) host1Sock.emit('pkAnswer', response)
    if (host2Sock) host2Sock.emit('pkAnswer', response)

    if (isAccept && host1Live && host2Live) {
      // Register the PK battle for score tracking
      pkBattles.set(host1Live, { host1UserId: host1Id, host2UserId: host2Id, host1Live, host2Live, score1: 0, score2: 0 })
      pkBattles.set(host2Live, { host1UserId: host1Id, host2UserId: host2Id, host1Live, host2Live, score1: 0, score2: 0 })
      log('pkBattle:start', `host1=${host1Id}(${host1Live}) vs host2=${host2Id}(${host2Live})`)

      // Also notify viewers in each room so they can join the opponent's channel
      // Exclude the hosts to avoid duplicate processing on their side
      broadcastToRoom(host2Live, 'pkAnswer', response, host2Sock?.id)  // viewers watching CHALLENGER
      broadcastToRoom(host1Live, 'pkAnswer', response, host1Sock?.id)  // viewers watching ACCEPTER
    }
  })

  // Gift sent during a PK battle — updates ranks for BOTH live rooms
  socket.on('pkGift', (raw) => {
    const data = parse(raw)
    const { liveHistoryId, receiverUserId, giftCount = 1 } = data

    let battle = pkBattles.get(liveHistoryId)
    if (!battle) {
      // Try to find the battle by checking if receiver is one of the PK hosts
      for (const b of pkBattles.values()) {
        if (b.host1UserId === receiverUserId || b.host2UserId === receiverUserId) { battle = b; break }
      }
    }

    if (battle) {
      // Update scores
      if (receiverUserId === battle.host1UserId) battle.score1 += giftCount
      else if (receiverUserId === battle.host2UserId) battle.score2 += giftCount

      // Build PK config from each room's perspective
      const host1Config = { localRank: battle.score1, remoteRank: battle.score2 }
      const host2Config = { localRank: battle.score2, remoteRank: battle.score1 }

      // Broadcast gift animation to both rooms
      broadcastToRoom(battle.host1Live, 'pkGift', { giftData: data, response: { pkConfig: host1Config } })
      broadcastToRoom(battle.host2Live, 'pkGift', { giftData: data, response: { pkConfig: host2Config } })

      // Update rank displays
      broadcastToRoom(battle.host1Live, 'pkRankUpdate', { pkConfig: host1Config, duration: 300 })
      broadcastToRoom(battle.host2Live, 'pkRankUpdate', { pkConfig: host2Config, duration: 300 })

      // Update top gifters
      broadcastToRoom(battle.host1Live, 'updateHostGifterStats', { topGiftersHost1: [], topGiftersHost2: [] })
      broadcastToRoom(battle.host2Live, 'updateHostGifterStats', { topGiftersHost1: [], topGiftersHost2: [] })

      log('pkGift', `to=${receiverUserId} scores=${battle.score1}:${battle.score2}`)
    } else {
      // No active PK, just broadcast to the room
      broadcastToRoom(liveHistoryId, 'pkGift', data)
    }
  })

  // End a PK battle (timer expired or manual)
  socket.on('pkEnd', (raw) => {
    const data = parse(raw)
    const { liveHistoryId, isManualMode, pkEndUserId } = data

    const battle = pkBattles.get(liveHistoryId)
    if (battle) {
      // Determine winner: score1 > score2 → host1 wins (isWinner: 2 means host2 wins)
      const isWinner = battle.score1 > battle.score2 ? 1 : battle.score2 > battle.score1 ? 2 : 0
      const result = {
        ...data,
        data: JSON.stringify({
          pkConfig: { isWinner, localRank: battle.score1, remoteRank: battle.score2 }
        }),
      }
      broadcastToRoom(battle.host1Live, 'pkEnd', result)
      broadcastToRoom(battle.host2Live, 'pkEnd', result)

      pkBattles.delete(battle.host1Live)
      pkBattles.delete(battle.host2Live)
      log('pkEnd', `liveHistoryId=${liveHistoryId} winner=${isWinner} scores=${battle.score1}:${battle.score2}`)
    } else {
      broadcastToRoom(liveHistoryId, 'pkEnd', data)
    }
  })

  // ── AUDIO ROOM ───────────────────────────────────────────────────────────────

  socket.on('hostJoinAudioRoom', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    addToRoom(liveHistoryId, socket.id)
    log('hostJoinAudioRoom', `user=${userId} room=${liveHistoryId}`)
  })

  socket.on('hostLeaveAudioRoom', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'endLiveStream', data)
    liveRooms.delete(liveHistoryId)
    log('hostLeaveAudioRoom', `user=${userId} room=${liveHistoryId}`)
  })

  socket.on('requestToJoinAudioRoom', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'seatUpdate', data)
  })

  socket.on('inviteToJoinRoom', (raw) => {
    const data = parse(raw)
    const { userId: targetId, liveHistoryId } = data
    if (targetId) emit(targetId, 'inviteToJoinRoom', { data: raw })
  })

  socket.on('audioRoomInviteRevoked', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'seatUpdate', data)
  })

  socket.on('participantAdded', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'seatUpdate', data)
    broadcastToRoom(liveHistoryId, 'seatedUsersUpdate', data)
    log('participantAdded', `room=${liveHistoryId}`)
  })

  socket.on('participantRemoved', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'seatUpdate', data)
    broadcastToRoom(liveHistoryId, 'participantRemoved', data)
  })

  socket.on('seatLocked', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'seatUpdate', data)
  })

  socket.on('seatMuted', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'seatUpdate', data)
  })

  socket.on('hostSeatMuted', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'seatUpdate', data)
  })

  socket.on('seatCountModified', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'seatUpdate', data)
  })

  socket.on('participantSpeaking', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'seatUpdate', data, socket.id)
  })

  socket.on('giftInAudioRoom', (raw) => {
    const data = parse(raw)
    const { liveHistoryId } = data
    if (!liveHistoryId) return
    broadcastToRoom(liveHistoryId, 'giftInAudioRoom', data)
    broadcastToRoom(liveHistoryId, 'liveRoomCoinUpdate', data)
  })

  // ── VIDEO CALLS ───────────────────────────────────────────────────────────────

  socket.on('initiateCall', (raw) => {
    const data = parse(raw)
    const { receiverId } = data
    if (!receiverId) return
    log('initiateCall', `from=${userId} to=${receiverId}`)
    const sent = emit(receiverId, 'incomingCall', data)
    if (!sent) emit(userId, 'initiateCall', { isOnline: false, isBusy: false, message: 'User is offline' })
  })

  socket.on('handleCallResponse', (raw) => {
    const data   = parse(raw)
    const { callerId, isAccept } = data
    log('handleCallResponse', `from=${userId} to=${callerId} accept=${isAccept}`)
    emit(callerId, isAccept ? 'callAcceptedByReceiver' : 'callRejectedByReceiver', raw)
  })

  socket.on('cancelOngoingCall', (raw) => {
    const data = parse(raw)
    const { receiverId, callerId } = data
    const target = receiverId === userId ? callerId : receiverId
    emit(target, 'callEnded', data)
    log('cancelOngoingCall', `user=${userId}`)
  })

  socket.on('callEnded', (raw) => {
    const data = parse(raw)
    const { callerId, receiverId } = data
    const target = callerId === userId ? receiverId : callerId
    emit(target, 'callEnded', data)
    log('callEnded', `from=${userId} to=${target}`)
  })

  socket.on('callCoinDeduction', (raw) => {
    const data = parse(raw)
    const { callerId, receiverId } = data
    const target = callerId === userId ? receiverId : callerId
    emit(target, 'insufficientCoins', data)
  })

  // ── CHAT MESSAGES ─────────────────────────────────────────────────────────────

  socket.on('messageSent', (raw) => {
    const data = parse(raw)
    const { receiverId } = data
    if (!receiverId) return
    emit(receiverId, 'messageSent', data)
  })

  socket.on('messageSeen', (raw) => {
    const data = parse(raw)
    const { senderId } = data
    if (!senderId) return
    emit(senderId, 'messageSeen', data)
  })

  // ── DISCONNECT ────────────────────────────────────────────────────────────────

  socket.on('disconnect', (reason) => {
    log('DISCONNECT', `user=${userId} socket=${socket.id} reason=${reason}`)
    if (userId) userSockets.delete(userId)
    // Notify all rooms this socket was in that the viewer left
    for (const [liveHistoryId, sockets] of liveRooms) {
      if (sockets.has(socket.id)) {
        removeViewer(liveHistoryId, userId)
        broadcastToRoom(liveHistoryId, 'liveViewersList', getViewerList(liveHistoryId))
      }
    }
    removeFromAllRooms(socket.id, userId)
  })

  socket.on('error', (err) => {
    log('ERROR', `user=${userId} error=${err.message}`)
  })
})

// ─── Start ─────────────────────────────────────────────────────────────────────

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 Tingle Socket.IO Server`)
  console.log(`   Port    : ${PORT}`)
  console.log(`   Health  : http://localhost:${PORT}/health`)
  console.log(`   Events  : live room, PK battle, audio room, calls, messages\n`)
})

process.on('uncaughtException', (err) => console.error('[FATAL]', err))
process.on('unhandledRejection', (err) => console.error('[REJECTION]', err))
