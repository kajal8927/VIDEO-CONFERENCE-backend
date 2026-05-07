require("dotenv").config();

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const rateLimit = require("express-rate-limit");

const app = express();

const ALLOWED_ORIGINS = [
  "http://localhost:5173",
  "http://localhost:5174",
  "http://localhost:5175",
  "http://localhost:5176",
  process.env.FRONTEND_URL,
  "https://video-conference-ozyy.onrender.com",
].filter(Boolean);

app.use(
  cors({
    origin: ALLOWED_ORIGINS,
    credentials: true,
  })
);

app.use(express.json());

const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  standardHeaders: true,
  legacyHeaders: false,
});

app.use(apiLimiter);

// Health check
app.get("/api/health", (req, res) => {
  res.json({
    status: "ok",
    message: "Backend is healthy",
    port: Number(process.env.PORT) || 5000,
  });
});

// Root route
app.get("/", (req, res) => {
  res.json({
    message: "Backend is working!",
    health: "/api/health",
  });
});

app.post("/api/summary", async (req, res) => {
  try {
    const {
      roomId,
      messages = [],
      speakingStats = {},
      meetingDuration = 0,
      participants = [],
    } = req.body || {};

    if (!roomId) {
      return res.status(400).json({
        success: false,
        message: "roomId is required",
      });
    }

    const safeMessages = Array.isArray(messages) ? messages : [];
    const safeParticipants = Array.isArray(participants) ? participants : [];
    const safeStats =
      speakingStats && typeof speakingStats === "object" ? speakingStats : {};

    const meaningfulMessages = safeMessages.filter(
      (m) => m?.text && String(m.text).trim().length > 2
    );

    const totalParticipants =
      safeParticipants.length || Object.keys(safeStats).length || 1;

    let dominantSpeaker = null;
    let maxTime = 0;

    for (const stat of Object.values(safeStats)) {
      const time = Number(stat?.totalSpeakingTime || 0);
      if (time > maxTime) {
        maxTime = time;
        dominantSpeaker = stat?.userName || "Participant";
      }
    }

    const durationMin = Math.floor(Number(meetingDuration || 0) / 60);
    const durationSec = Number(meetingDuration || 0) % 60;

    const overview = `Room ID: ${roomId} • Duration: ${durationMin}m ${durationSec}s • Total Participants: ${totalParticipants} • Total Messages: ${safeMessages.length}${
      dominantSpeaker ? ` • Most Active Speaker: ${dominantSpeaker}` : ""
    }`;

    const fullText = meaningfulMessages
      .map((m) => String(m.text).toLowerCase())
      .join(" ");

    const keyPoints = [];

    if (meaningfulMessages.length === 0) {
      keyPoints.push("No meaningful discussion data available for summary.");
    } else {
      if (
        fullText.includes("hello") ||
        fullText.includes("hi") ||
        fullText.includes("hey")
      ) {
        keyPoints.push("Participants greeted each other.");
      }

      if (
        fullText.includes("my name is") ||
        fullText.includes("i am") ||
        fullText.includes("i'm")
      ) {
        keyPoints.push("Participants introduced themselves.");
      }

      keyPoints.push("Discussion was based on messages shared during the meeting.");
    }

    const actionWords = [
      "do",
      "complete",
      "finish",
      "submit",
      "prepare",
      "send",
      "schedule",
      "follow up",
    ];

    let actionItems = meaningfulMessages
      .filter((m) =>
        actionWords.some((word) =>
          String(m.text || "").toLowerCase().includes(word)
        )
      )
      .map(
        (m) =>
          `${m.senderName || m.sender || "Participant"}: ${String(
            m.text
          ).trim()}`
      );

    if (actionItems.length === 0) {
      actionItems = ["No action items identified."];
    }

    return res.status(200).json({
      success: true,
      summary: overview,
      overview,
      keyPoints: keyPoints.slice(0, 3),
      actionItems,
      participantInsights: [
        dominantSpeaker
          ? `Most active speaker: ${dominantSpeaker}`
          : "No clear dominant speaker identified.",
        `Total participants: ${totalParticipants}`,
      ],
    });
  } catch (error) {
    console.error("Summary Generation Error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to generate summary",
      error: error.message,
    });
  }
});
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: ALLOWED_ORIGINS,
    methods: ["GET", "POST"],
    credentials: true,
  },
});

// ─── State ────────────────────────────────────────────────────────────────────
const socketRateLimits = {};
const rooms = {}; // { [roomId]: { users: { [socketId]: UserObj }, host: socketId, locked: bool } }
const waitingRooms = {}; // { [roomId]: { [socketId]: { socketId, userName } } }

// ─── Helpers ──────────────────────────────────────────────────────────────────

const checkSocketRateLimit = (socketId) => {
  const now = Date.now();
  if (!socketRateLimits[socketId]) socketRateLimits[socketId] = [];
  socketRateLimits[socketId] = socketRateLimits[socketId].filter(
    (t) => now - t < 1000
  );
  if (socketRateLimits[socketId].length >= 10) return false;
  socketRateLimits[socketId].push(now);
  return true;
};

const sanitizeString = (value, maxLength = 200) => {
  if (typeof value !== "string") return "";
  return value.trim().substring(0, maxLength);
};

function getRoomParticipants(roomId) {
  if (!rooms[roomId]) return [];
  return Object.values(rooms[roomId].users).map((user) => ({
    socketId: user.socketId,
    userName: user.userName,
    isHost: rooms[roomId].host === user.socketId,
    raisedHand: !!user.raisedHand,
  }));
}

function broadcastStats(roomId) {
  const roomUsers = rooms[roomId]?.users;
  if (!roomUsers) return;

  const stats = {};
  const now = Date.now();

  for (const socketId in roomUsers) {
    const user = roomUsers[socketId];
    let currentSpeakingTime = user.totalSpeakingTime;
    if (user.isSpeaking && user.lastStartedSpeakingAt) {
      currentSpeakingTime += now - user.lastStartedSpeakingAt;
    }
    stats[socketId] = {
      userName: user.userName,
      totalSpeakingTime: currentSpeakingTime,
      isSpeaking: user.isSpeaking,
    };
  }

  io.to(roomId).emit("speaking-stats", stats);
}

/**
 * Remove a socket from every room it appears in.
 * Called on disconnect — handles edge cases where socket.roomId was never set.
 */
function removeFromAllRooms(socketId) {
  for (const roomId of Object.keys(waitingRooms)) {
    if (waitingRooms[roomId][socketId]) {
      delete waitingRooms[roomId][socketId];
    }
  }

  for (const roomId of Object.keys(rooms)) {
    const room = rooms[roomId];
    if (!room.users[socketId]) continue;

    const user = room.users[socketId];

    // Finalize speaking time
    if (user.isSpeaking && user.lastStartedSpeakingAt) {
      user.totalSpeakingTime += Date.now() - user.lastStartedSpeakingAt;
    }

    console.log(
      `[LEAVE] ${user.userName} (${socketId}) left room ${roomId}`
    );

    delete room.users[socketId];

    io.to(roomId).emit("user-left", socketId);
    io.to(roomId).emit("participants-update", getRoomParticipants(roomId));

    // Reassign host if needed
    if (room.host === socketId) {
      const remaining = Object.keys(room.users);
      if (remaining.length > 0) {
        room.host = remaining[0];
        io.to(remaining[0]).emit("host-status", true);
        io.to(roomId).emit("participants-update", getRoomParticipants(roomId));
        console.log(
          `[LEAVE] Host reassigned to ${room.users[remaining[0]]?.userName} in room ${roomId}`
        );
        // Send pending requests to new host
        if (waitingRooms[roomId]) {
          const pendingRequests = Object.values(waitingRooms[roomId]);
          pendingRequests.forEach(req => {
            io.to(remaining[0]).emit("join-request-received", req);
          });
        }
      }
    }

    if (Object.keys(room.users).length === 0) {
      console.log(`[LEAVE] Room ${roomId} is empty — deleted.`);
      delete rooms[roomId];
    } else {
      broadcastStats(roomId);
    }
  }
}

/**
 * Every 10 s: cross-check room user lists against actually-connected sockets.
 * Removes any entry whose socket is no longer connected (crash / silent drop).
 */
function sweepStaleUsers() {
  const connectedIds = new Set(io.sockets.sockets.keys());

  for (const roomId of Object.keys(rooms)) {
    const room = rooms[roomId];
    const staleIds = Object.keys(room.users).filter(
      (id) => !connectedIds.has(id)
    );

    if (staleIds.length === 0) continue;

    for (const staleId of staleIds) {
      console.log(
        `[SWEEP] Removing stale ${room.users[staleId]?.userName} (${staleId}) from room ${roomId}`
      );
      delete room.users[staleId];
    }

    // Reassign host if it was stale
    if (!room.users[room.host]) {
      const remaining = Object.keys(room.users);
      if (remaining.length > 0) {
        room.host = remaining[0];
        io.to(remaining[0]).emit("host-status", true);
        console.log(
          `[SWEEP] Host reassigned to ${room.users[remaining[0]]?.userName} in room ${roomId}`
        );
      }
    }

    io.to(roomId).emit("participants-update", getRoomParticipants(roomId));

    if (Object.keys(room.users).length === 0) {
      console.log(`[SWEEP] Room ${roomId} is empty after sweep — deleted.`);
      delete rooms[roomId];
    }
  }
}

// ─── Socket.IO ────────────────────────────────────────────────────────────────

io.on("connection", (socket) => {
  console.log(`[CONNECT] ${socket.id}`);

  // ── request-join-room (Waiting Room Flow) ──────────────────────────────────
  socket.on("request-join-room", (rawRoomId, rawUserName) => {
    if (!checkSocketRateLimit(socket.id)) return;

    const roomId = sanitizeString(rawRoomId, 50);
    const userName = sanitizeString(rawUserName, 50) || "Anonymous";

    if (!roomId) return;

    if (!rooms[roomId]) {
      // Room doesn't exist, this user is the first and therefore the host!
      socket.emit("user-admitted");
    } else {
      // Room exists, send them to waiting room and alert host
      if (!waitingRooms[roomId]) waitingRooms[roomId] = {};
      
      // Prevent duplicate requests
      if (!waitingRooms[roomId][socket.id]) {
        waitingRooms[roomId][socket.id] = { socketId: socket.id, userName };
        console.log(`[WAITING] ${userName} (${socket.id}) is waiting for room ${roomId}`);
        
        const hostSocketId = rooms[roomId].host;
        if (hostSocketId) {
          io.to(hostSocketId).emit("join-request-received", { socketId: socket.id, userName });
        }
      }
    }
  });

  // ── Host Waiting Room Actions ────────────────────────────────────────────────
  socket.on("admit-user", (rawRoomId, targetSocketId) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (!rooms[roomId] || rooms[roomId].host !== socket.id) return;
    
    if (waitingRooms[roomId] && waitingRooms[roomId][targetSocketId]) {
      delete waitingRooms[roomId][targetSocketId];
      io.to(targetSocketId).emit("user-admitted");
      console.log(`[ADMIT] Host admitted ${targetSocketId} to room ${roomId}`);
    }
  });

  socket.on("deny-user", (rawRoomId, targetSocketId) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (!rooms[roomId] || rooms[roomId].host !== socket.id) return;
    
    if (waitingRooms[roomId] && waitingRooms[roomId][targetSocketId]) {
      delete waitingRooms[roomId][targetSocketId];
      io.to(targetSocketId).emit("user-denied");
      console.log(`[DENY] Host denied ${targetSocketId} for room ${roomId}`);
    }
  });

  socket.on("host-camera-off-participant", (rawRoomId, targetSocketId) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (!rooms[roomId] || rooms[roomId].host !== socket.id) return;
    io.to(targetSocketId).emit("camera-off-requested");
  });

  // ── join-room ──────────────────────────────────────────────────────────────
  socket.on("join-room", (rawRoomId, rawUserName) => {
    if (!checkSocketRateLimit(socket.id)) return;

    const roomId = sanitizeString(rawRoomId, 50);
    const userName = sanitizeString(rawUserName, 50) || "Anonymous";

    if (!roomId) return;

    if (rooms[roomId]?.locked) {
      socket.emit("room-locked");
      return;
    }

    // Create room if needed
    if (!rooms[roomId]) {
      rooms[roomId] = { users: {}, host: socket.id, locked: false };
    }

    const roomUsers = rooms[roomId].users;

    // Guard: if same socketId already present, just log — entry will be overwritten below
    if (roomUsers[socket.id]) {
      console.log(
        `[JOIN] Socket ${socket.id} re-joining room ${roomId} — overwriting entry.`
      );
    }

    // Remove ghost: same userName, different socketId
    for (const existingId of Object.keys(roomUsers)) {
      if (
        existingId !== socket.id &&
        roomUsers[existingId].userName === userName
      ) {
        console.log(
          `[JOIN] Ghost cleanup — "${userName}" already in room with old socket ${existingId}. Removing.`
        );
        io.to(existingId).emit("duplicate-session-closed");
        delete roomUsers[existingId];
      }
    }

    // Add / overwrite entry
    roomUsers[socket.id] = {
      socketId: socket.id,
      userName,
      totalSpeakingTime: 0,
      isSpeaking: false,
      lastStartedSpeakingAt: null,
      raisedHand: false,
    };

    socket.join(roomId);
    socket.roomId = roomId;
    socket.userName = userName;

    const existingUsers = Object.keys(roomUsers).filter(
      (id) => id !== socket.id
    );
    const isHost = rooms[roomId].host === socket.id;

    console.log(
      `[JOIN] ${userName} (${socket.id}) → room ${roomId} | total: ${
        Object.keys(roomUsers).length
      }`
    );

    // Emit to joiner
    socket.emit("room-users", existingUsers);
    socket.emit("host-status", isHost);
    socket.emit("participants-update", getRoomParticipants(roomId));

    // Emit to everyone else
    socket.to(roomId).emit("user-joined", socket.id, userName);
    socket.to(roomId).emit("participants-update", getRoomParticipants(roomId));

    broadcastStats(roomId);
  });

  // ── WebRTC signaling ───────────────────────────────────────────────────────
  socket.on("offer", (targetId, offer) => {
    socket.to(targetId).emit("offer", socket.id, offer);
  });

  socket.on("answer", (targetId, answer) => {
    socket.to(targetId).emit("answer", socket.id, answer);
  });

  socket.on("ice-candidate", (targetId, candidate) => {
    socket.to(targetId).emit("ice-candidate", socket.id, candidate);
  });

  // ── Screen share ───────────────────────────────────────────────────────────
  socket.on("screen-share-started", (rawRoomId) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (roomId) socket.to(roomId).emit("user-screen-share-started", socket.id);
  });

  socket.on("screen-share-stopped", (rawRoomId) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (roomId) socket.to(roomId).emit("user-screen-share-stopped", socket.id);
  });

  // ── Chat ───────────────────────────────────────────────────────────────────
  socket.on("chat-message", (rawRoomId, rawMessage) => {
    if (!checkSocketRateLimit(socket.id)) return;
    const roomId = sanitizeString(rawRoomId, 50);
    if (!roomId || !rawMessage) return;
    socket.to(roomId).emit("chat-message", socket.id, rawMessage);
  });

  // ── Speaking state ─────────────────────────────────────────────────────────
  socket.on("speaking-state-change", (rawRoomId, isSpeaking) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (!rooms[roomId]?.users[socket.id]) return;

    const user = rooms[roomId].users[socket.id];
    const now = Date.now();

    if (isSpeaking && !user.isSpeaking) {
      user.isSpeaking = true;
      user.lastStartedSpeakingAt = now;
    } else if (!isSpeaking && user.isSpeaking) {
      user.isSpeaking = false;
      if (user.lastStartedSpeakingAt) {
        user.totalSpeakingTime += now - user.lastStartedSpeakingAt;
        user.lastStartedSpeakingAt = null;
      }
    }
  });

  // ── Mute Toggle ────────────────────────────────────────────────────────────
  socket.on("user-toggle-mute", (rawRoomId, data) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (!roomId) return;
    socket.to(roomId).emit("user-toggle-mute", data);
  });

  // ── Video Toggle ───────────────────────────────────────────────────────────
  socket.on("user-toggle-video", (rawRoomId, data) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (!roomId) return;
    socket.to(roomId).emit("user-toggle-video", data);
  });

  // ── Raise hand ─────────────────────────────────────────────────────────────
  socket.on("raise-hand", (rawRoomId, raisedHand) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (!rooms[roomId]?.users[socket.id]) return;
    rooms[roomId].users[socket.id].raisedHand = !!raisedHand;
    io.to(roomId).emit("raise-hand-update", {
      socketId: socket.id,
      raisedHand: !!raisedHand,
    });
  });

  // ── Reactions ──────────────────────────────────────────────────────────────
  socket.on("send-reaction", (rawRoomId, emoji, senderName) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (!roomId) return;
    io.to(roomId).emit("receive-reaction", {
      id: `${socket.id}-${Date.now()}`,
      reaction: emoji,
      senderName: sanitizeString(senderName, 50),
    });
  });

  // ── Host controls ──────────────────────────────────────────────────────────
  socket.on("host-lock-room", (rawRoomId) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (!rooms[roomId] || rooms[roomId].host !== socket.id) return;
    rooms[roomId].locked = true;
    io.to(roomId).emit("room-lock-status", true);
  });

  socket.on("host-unlock-room", (rawRoomId) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (!rooms[roomId] || rooms[roomId].host !== socket.id) return;
    rooms[roomId].locked = false;
    io.to(roomId).emit("room-lock-status", false);
  });

  socket.on("host-mute-participant", (rawRoomId, targetSocketId) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (!rooms[roomId] || rooms[roomId].host !== socket.id) return;
    io.to(targetSocketId).emit("mute-requested");
  });

  socket.on("host-remove-participant", (rawRoomId, targetSocketId) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (!rooms[roomId] || rooms[roomId].host !== socket.id) return;

    io.to(targetSocketId).emit("you-were-removed");

    if (rooms[roomId]?.users[targetSocketId]) {
      delete rooms[roomId].users[targetSocketId];
      io.to(roomId).emit("user-left", targetSocketId);
      io.to(roomId).emit("participants-update", getRoomParticipants(roomId));
      console.log(`[HOST] Kicked ${targetSocketId} from room ${roomId}`);
    }
  });

  socket.on("host-end-meeting", (rawRoomId) => {
    const roomId = sanitizeString(rawRoomId, 50);
    if (!rooms[roomId] || rooms[roomId].host !== socket.id) return;
    io.to(roomId).emit("meeting-ended-by-host");
    delete rooms[roomId];
    console.log(`[HOST] Meeting ended for room ${roomId}`);
  });

  // ── Disconnect ─────────────────────────────────────────────────────────────
  socket.on("disconnect", () => {
    delete socketRateLimits[socket.id];
    console.log(
      `[DISCONNECT] ${socket.userName || "unknown"} (${socket.id})`
    );
    removeFromAllRooms(socket.id);
  });
});

// ─── Intervals ────────────────────────────────────────────────────────────────

// Broadcast speaking stats every second
setInterval(() => {
  for (const roomId in rooms) {
    broadcastStats(roomId);
  }
}, 1000);

// Sweep stale/ghost users every 10 seconds
setInterval(sweepStaleUsers, 10000);

// ─── Error handler & start ───────────────────────────────────────────────────

app.use((err, req, res, next) => {
  console.error("Global error handler:", err.stack);
  res.status(500).json({ error: "Something went wrong on the server." });
});

const PORT = Number(process.env.PORT) || 5000;

server.listen(PORT, () => {
  console.log(`Backend signaling server running on port ${PORT}`);
});