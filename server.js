import express from "express";
import http from "http";
import { Server } from "socket.io";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const { TikTokLiveConnection, ControlEvent, ControlAction } = require("tiktok-live-connector");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: process.env.ALLOWED_ORIGIN || "*",
        methods: ["GET", "POST"]
    }
});

// 🔧 Configuração
const username = process.env.TIKTOK_USER || "usuario";
const PORT = process.env.PORT || 3000;

const RETRY_WHEN_OFFLINE = 30000;   // tenta de novo a cada 30s se offline
const RETRY_AFTER_ERROR = 60000;    // espera mais se for erro de rede/rate-limit
const HEARTBEAT_INTERVAL = 60000;   // confirma o status a cada 1 min mesmo já "conectado"

// 🔥 Estado
let isLive = false;
let connection = null;
let retryTimer = null;
let heartbeatTimer = null;
let isConnecting = false;
let lastCheck = null;

// 📡 Atualiza e notifica (status + timestamp da verificação)
function setLive(status) {
    lastCheck = new Date().toISOString();

    if (status !== isLive) {
        isLive = status;
        console.log(`[${lastCheck}] ${username} →`, isLive ? "🟢 AO VIVO" : "🔴 OFFLINE");
    }

    io.emit("liveStatus", { user: username, online: isLive, lastCheck });
}

function scheduleRetry(delay) {
    clearTimeout(retryTimer);
    retryTimer = setTimeout(connectToLive, delay);
}

// 🧹 Limpa a conexão anterior antes de criar uma nova
function cleanupConnection() {
    clearInterval(heartbeatTimer);
    if (connection) {
        connection.removeAllListeners();
        try { connection.disconnect(); } catch (_) { /* já pode estar desconectado */ }
        connection = null;
    }
}

// 💓 Enquanto "conectado", confirma periodicamente via API (pega quedas silenciosas)
function startHeartbeat() {
    clearInterval(heartbeatTimer);
    heartbeatTimer = setInterval(async () => {
        if (!connection) return;
        try {
            const roomInfo = await connection.fetchRoomInfo();
            // status !== 2 normalmente indica que a sala não está mais ao vivo
            const stillLive = roomInfo?.status === 2;
            if (!stillLive) {
                console.log("💓 Heartbeat detectou queda silenciosa da live");
                setLive(false);
                cleanupConnection();
                scheduleRetry(RETRY_WHEN_OFFLINE);
            } else {
                setLive(true); // só atualiza o lastCheck, status já é true
            }
        } catch (err) {
            console.log("💓 Heartbeat falhou:", err.message);
            // não muda o status por causa de uma falha isolada de rede
        }
    }, HEARTBEAT_INTERVAL);
}

// 🔍 Tenta conectar à live do usuário
function connectToLive() {
    if (isConnecting) return; // evita conexões duplicadas em paralelo
    isConnecting = true;

    cleanupConnection();

    connection = new TikTokLiveConnection(username, {
        requestOptions: { timeout: 10000 },
        processInitialData: false
    });

    connection.on(ControlEvent.CONNECTED, (state) => {
        isConnecting = false;
        console.log(`Conectado à sala ${state.roomId} de @${username}`);
        setLive(true);
        startHeartbeat();
    });

    connection.on(ControlEvent.DISCONNECTED, ({ code, reason }) => {
        console.log(`Desconectado (code: ${code})${reason ? `, motivo: ${reason}` : ""}`);
        setLive(false);
        clearInterval(heartbeatTimer);
        scheduleRetry(RETRY_WHEN_OFFLINE);
    });

    connection.on(ControlEvent.STREAM_END, ({ action }) => {
        const motivo =
            action === ControlAction.CONTROL_ACTION_STREAM_SUSPENDED
                ? "(banida/moderada)"
                : "(encerrada pelo host)";
        console.log(`@${username} encerrou a live ${motivo}`);
    });

    // Sem isso, um 'error' não tratado pode derrubar o processo inteiro
    connection.on("error", (err) => {
        console.log("Erro na conexão com a live:", err?.message || err);
    });

    connection.connect()
        .catch(err => {
            isConnecting = false;
            setLive(false);

            const isRateLimit = /rate.?limit|429/i.test(err.message || "");
            console.log(
                isRateLimit
                    ? `⚠️ Rate limit detectado, aumentando intervalo: ${err.message}`
                    : `Offline ou erro ao conectar (${err.name || "Error"}): ${err.message}`
            );

            scheduleRetry(isRateLimit ? RETRY_AFTER_ERROR * 2 : RETRY_WHEN_OFFLINE);
        });
}

// 📡 Envia o estado atual para quem acabou de conectar no socket.io
io.on("connection", (socket) => {
    socket.emit("liveStatus", { user: username, online: isLive, lastCheck });
});

app.use(express.static("public"));

server.listen(PORT, () => {
    console.log(`Servidor rodando em http://localhost:${PORT}`);
    console.log(`Monitorando: @${username}`);
    connectToLive();
});

process.on("unhandledRejection", (reason) => {
    console.error("⚠️ Unhandled Rejection:", reason);
});

process.on("uncaughtException", (err) => {
    console.error("⚠️ Uncaught Exception:", err);
});

process.on("SIGTERM", () => {
    clearTimeout(retryTimer);
    clearInterval(heartbeatTimer);
    server.close(() => process.exit(0));
});

process.on("SIGINT", () => {
    clearTimeout(retryTimer);
    clearInterval(heartbeatTimer);
    server.close(() => process.exit(0));
});