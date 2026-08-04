import express from "express";
import http from "http";
import { Server } from "socket.io";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const { TikTokLiveConnection, ControlEvent } = require("tiktok-live-connector");

require("dotenv").config();

const app = express();
const server = http.createServer(app);

// ✅ CORS CORRIGIDO
const allowedOrigins = (process.env.ALLOWED_ORIGINS || "*")
    .split(",")
    .map(o => o.trim())
    .filter(Boolean)
    .concat([
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5000",
        "http://127.0.0.1:5000"
    ]);

const io = new Server(server, {
    cors: {
        origin: (origin, callback) => {
            if (!origin) return callback(null, true);
            if (allowedOrigins.includes("*") || allowedOrigins.includes(origin)) {
                callback(null, true);
            } else {
                callback(new Error("Not allowed by CORS"));
            }
        }
    }
});

const username = process.env.TIKTOK_USER;
if (!username) {
    console.error("❌ TIKTOK_USER não configurado.");
    process.exit(1);
}

const PORT = process.env.PORT || 3000;

// CONFIG
const HEARTBEAT_INTERVAL = 30000;
const ACTIVITY_WINDOW = 180000;
const INITIAL_GRACE = 120000;

// ESTADO
let isLive = false;
let connection = null;
let heartbeatTimer = null;
let isConnecting = false;
let connectedAt = null;
let lastActivity = 0;

// STATUS
function setLive(status) {
    if (status === isLive) return;

    isLive = status;

    console.log(`[STATUS] ${username} → ${status ? "🟢 LIVE" : "🔴 OFFLINE"}`);

    io.emit("liveStatus", {
        user: username,
        online: isLive,
        lastCheck: new Date().toISOString()
    });
}

// LIMPEZA
function cleanupConnection() {
    clearInterval(heartbeatTimer);

    if (connection) {
        connection.removeAllListeners();
        try { connection.disconnect(); } catch { }
        connection = null;
    }
}

// HEARTBEAT ESTÁVEL
function startHeartbeat() {
    clearInterval(heartbeatTimer);

    heartbeatTimer = setInterval(() => {
        if (!connection) return;

        const now = Date.now();
        const timeSinceActivity = now - lastActivity;
        const timeSinceConnect = now - (connectedAt || now);

        const hasRecentActivity =
            lastActivity > 0 && timeSinceActivity < ACTIVITY_WINDOW;

        const stillLive =
            hasRecentActivity ||
            timeSinceConnect < INITIAL_GRACE;

        console.log({
            activity: lastActivity ? Math.floor(timeSinceActivity / 1000) + "s" : "nunca",
            connected: Math.floor(timeSinceConnect / 1000) + "s"
        });

        if (stillLive) {
            setLive(true);
            return;
        }

        console.log("💀 Sem sinais → reconectando");

        setLive(false);
        cleanupConnection();
        setTimeout(connectToLive, 10000); // 🔥 delay pra evitar loop

    }, HEARTBEAT_INTERVAL);
}

// CONEXÃO
function connectToLive() {
    if (isConnecting || connection) return;

    isConnecting = true;
    cleanupConnection();

    connection = new TikTokLiveConnection(username, {
        requestOptions: { timeout: 8000 },
        processInitialData: false
    });

    const markActivity = () => {
        lastActivity = Date.now();
    };

    connection.on("chat", markActivity);
    connection.on("like", markActivity);
    connection.on("gift", markActivity);
    connection.on("member", markActivity);

    connection.once(ControlEvent.CONNECTED, async (state) => {
        isConnecting = false;
        connectedAt = Date.now();

        console.log(`✅ Conectado sala ${state.roomId}`);

        try {
            const roomInfo = await connection.fetchRoomInfo();

            if (roomInfo?.status === 2) {
                setLive(true);
            } else {
                console.log("⚠️ Aguardando atividade...");
            }
        } catch { }

        startHeartbeat();
    });

    connection.once(ControlEvent.DISCONNECTED, () => {
        console.log("🔌 Desconectado");

        isConnecting = false;
        setLive(false);

        cleanupConnection();
        setTimeout(connectToLive, 30000);
    });

    connection.on("error", (err) => {
        console.error("Erro:", err.message);
    });

    connection.connect().catch(err => {
        isConnecting = false;
        console.log("❌ Erro:", err.message);
        setTimeout(connectToLive, 30000);
    });
}

// SOCKET
io.on("connection", (socket) => {
    socket.emit("liveStatus", {
        user: username,
        online: isLive
    });
});

// API
app.get("/health", (req, res) => {
    res.json({
        live: isLive,
        connected: !!connection
    });
});

app.use(express.static("public"));

// START (APENAS UMA VEZ)
server.listen(PORT, () => {
    console.log(`🚀 http://localhost:${PORT}`);
    console.log(`Monitorando @${username}`);
    connectToLive();
});

// 🔥 Anti-hibernação leve (Render safe)
setInterval(() => {
    if (!connection && !isConnecting) {
        console.log("♻️ Reconectando (keep alive)");
        connectToLive();
    }
}, 60000);