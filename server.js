const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const rateLimit = require("express-rate-limit");

const app = express();

const ALLOWED_ORIGIN = process.env.FRONTEND_URL || "http://localhost:5173";

app.use(
  cors({
    origin: ALLOWED_ORIGIN,
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

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: ALLOWED_ORIGIN,
    methods: ["GET", "POST"],
  },
});
app.get('/', (req, res) => {
  res.send('Backend is working!');
});

const socketRateLimits = {};

const checkSocketRateLimit = (socketId) => {
  const now = Date.now();

  if (!socketRateLimits[socketId]) {
    socketRateLimits[socketId] = [];
  }

  socketRateLimits[socketId] = socketRateLimits[socketId].filter(
    (time) => now - time < 1000
  );

  if (socketRateLimits[socketId].length >= 10) {
    return false;
  }

  socketRateLimits[socketId].push(now);
  return true;
};

const sanitizeString = (value, maxLength = 200) => {
  if (typeof value !== "string") return "";
  return value.trim().substring(0, maxLength);
};

// rooms[roomId] = {
//   users: {
//     socketId: {
//       socketId,
//       userName,
//       totalSpeakingTime,
//       isSpeaking,
//       lastStartedSpeakingAt,
//       raisedHand
//     }
//   },
//   host: socketId,
//   locked: false
// }

const rooms = {};

function getRoomParticipants(roomId) {
  if (!rooms[roomId]) return [];

  return Object.values(rooms[roomId].users).map((user) => ({
    socketId: user.socketId,
    userName: user.userName,
    isHost: rooms[roomId].host === user.socketId,
    raisedHand: !!user.raisedHand,
  }));
}

io.on("connection", (socket) => {
  console.log(`User connected: ${socket.id}`);

  socket.on("join-room", (rawRoomId, rawUserName) => {
    if (!checkSocketRateLimit(socket.id)) return;

    const roomId = sanitizeString(rawRoomId, 50);
    const userName = sanitizeString(rawUserName, 50) || "Anonymous";

    if (!roomId) return;

    if (rooms[roomId]?.locked) {
      socket.emit("room-locked");
      return;
    }

    socket.join(roomId);

    if (!rooms[roomId]) {
      rooms[roomId] = {
        users: {},
        host: socket.id,
        locked: false,
      };
    }

    rooms[roomId].users[socket.id] = {
      socketId: socket.id,
      userName,
      totalSpeakingTime: 0,
      isSpeaking: false,
      lastStartedSpeakingAt: null,
      raisedHand: false,
    };

    socket.roomId = roomId;
    socket.userName = userName;

    const existingUsers = Object.keys(rooms[roomId].users).filter(
      (id) => id !== socket.id
    );

    const isHost = rooms[roomId].host === socket.id;

    socket.emit("room-users", existingUsers);
    socket.emit("host-status", isHost);
    socket.emit("participants-update", getRoomParticipants(roomId));

    socket.to(roomId).emit("user-joined", socket.id, userName);
    socket.to(roomId).emit("participants-update", getRoomParticipants(roomId));

    console.log(
      `${userName} (${socket.id}) joined room ${roomId} | host: ${rooms[roomId].host}`
    );

    broadcastStats(roomId);
  });

  socket.on("offer", (targetId, offer) => {
    socket.to(targetId).emit("offer", socket.id, offer);
  });

  socket.on("answer", (targetId, answer) => {
    socket.to(targetId).emit("answer", socket.id, answer);
  });

  socket.on("ice-candidate", (targetId, candidate) => {
    socket.to(targetId).emit("ice-candidate", socket.id, candidate);
  });

  socket.on("chat-message", (rawRoomId, rawMessage) => {
    if (!checkSocketRateLimit(socket.id)) return;

    const roomId = sanitizeString(rawRoomId, 50);

    if (!roomId || !rawMessage) return;

    socket.to(roomId).emit("chat-message", socket.id, rawMessage);
  });

  socket.on("send-reaction", (rawRoomId, rawReaction, rawSenderName) => {
    if (!checkSocketRateLimit(socket.id)) return;

    const roomId = sanitizeString(rawRoomId, 50);
    const reaction = sanitizeString(rawReaction, 10);
    const senderName = sanitizeString(rawSenderName, 50);

    if (!roomId || !reaction) return;

    io.to(roomId).emit("receive-reaction", {
      id: Date.now() + Math.random(),
      reaction,
      senderName,
      socketId: socket.id,
    });
  });

  socket.on("speaking-state-change", (rawRoomId, isSpeaking) => {
    const roomId = sanitizeString(rawRoomId, 50);

    if (!rooms[roomId] || !rooms[roomId].users[socket.id]) return;

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

  socket.on("host-remove-participant", (rawRoomId, targetSocketId) => {
    const roomId = sanitizeString(rawRoomId, 50);

    if (!rooms[roomId] || rooms[roomId].host !== socket.id) return;
    if (!rooms[roomId].users[targetSocketId]) return;

    io.to(targetSocketId).emit("you-were-removed");

    const targetSocket = io.sockets.sockets.get(targetSocketId);

    if (targetSocket) {
      targetSocket.leave(roomId);
      targetSocket.disconnect(true);
    }

    delete rooms[roomId].users[targetSocketId];

    io.to(roomId).emit("user-left", targetSocketId);
    io.to(roomId).emit("participants-update", getRoomParticipants(roomId));
  });

  socket.on("host-end-meeting", (rawRoomId) => {
    const roomId = sanitizeString(rawRoomId, 50);

    if (!rooms[roomId] || rooms[roomId].host !== socket.id) return;

    io.to(roomId).emit("meeting-ended-by-host");

    Object.keys(rooms[roomId].users).forEach((socketId) => {
      const participantSocket = io.sockets.sockets.get(socketId);
      if (participantSocket) {
        participantSocket.leave(roomId);
      }
    });

    delete rooms[roomId];

    console.log(`Host ended meeting in room ${roomId}`);
  });

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
    if (!rooms[roomId].users[targetSocketId]) return;

    io.to(targetSocketId).emit("mute-requested");
  });

  socket.on("raise-hand", (rawRoomId, isRaised) => {
    if (!checkSocketRateLimit(socket.id)) return;

    const roomId = sanitizeString(rawRoomId, 50);

    if (!roomId || typeof isRaised !== "boolean") return;
    if (!rooms[roomId] || !rooms[roomId].users[socket.id]) return;

    rooms[roomId].users[socket.id].raisedHand = isRaised;

    io.to(roomId).emit("participants-update", getRoomParticipants(roomId));
    io.to(roomId).emit("raise-hand-update", {
      socketId: socket.id,
      raisedHand: isRaised,
    });
  });

  socket.on("disconnect", () => {
    delete socketRateLimits[socket.id];

    console.log(`User disconnected: ${socket.id}`);

    const roomId = socket.roomId;

    if (!roomId || !rooms[roomId]) return;

    const user = rooms[roomId].users[socket.id];

    if (user?.isSpeaking && user.lastStartedSpeakingAt) {
      user.totalSpeakingTime += Date.now() - user.lastStartedSpeakingAt;
    }

    delete rooms[roomId].users[socket.id];

    socket.to(roomId).emit("user-left", socket.id);
    socket.to(roomId).emit("participants-update", getRoomParticipants(roomId));

    if (rooms[roomId].host === socket.id) {
      const remainingUsers = Object.keys(rooms[roomId].users);

      if (remainingUsers.length > 0) {
        rooms[roomId].host = remainingUsers[0];

        io.to(remainingUsers[0]).emit("host-status", true);
        io.to(roomId).emit(
          "participants-update",
          getRoomParticipants(roomId)
        );

        console.log(`Host transferred to ${remainingUsers[0]} in room ${roomId}`);
      }
    }

    if (Object.keys(rooms[roomId].users).length === 0) {
      delete rooms[roomId];
    } else {
      broadcastStats(roomId);
    }
  });
});

setInterval(() => {
  for (const roomId in rooms) {
    broadcastStats(roomId);
  }
}, 1000);

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

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  console.log(`Backend signaling server running on port ${PORT}`);
});