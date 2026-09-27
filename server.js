import "dotenv/config";
import express from "express";
import http from "http";
import { Server } from "socket.io";
import { TikTokLiveConnection, SignConfig } from "tiktok-live-connector";

const app = express();
const server = http.createServer(app);

// CORS
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

const username = (process.env.TIKTOK_USER || "").trim().replace(/^@/, "");
if (!username) {
    console.error("❌ TIKTOK_USER não configurado.");
    process.exit(1);
}

// Opcional: chave da Euler Stream (evita limite do fallback). https://www.eulerstream.com
if (process.env.SIGN_API_KEY) SignConfig.apiKey = process.env.SIGN_API_KEY;

const PORT = process.env.PORT || 3000;

// CONFIG
const CHECK_INTERVAL = Number(process.env.CHECK_INTERVAL_MS) || 30000; // intervalo entre verificações
const OFFLINE_CONFIRMATIONS = 2; // quantas checagens "offline" seguidas antes de marcar offline (evita piscar)

// ESTADO
let isLive = false;
let lastCheck = null;
let lastError = null;
let offlineStreak = 0;
let checking = false;

// Uma única instância, usada só para as consultas HTTP (não abre WebSocket)
const client = new TikTokLiveConnection(username, {
    webClientOptions: { timeout: { request: 10000 } }
});
client.on("error", ({ info, exception }) => {
    console.error("Erro (lib):", info, exception?.message || exception);
});

function payload() {
    return {
        user: username,
        online: isLive,
        lastCheck: lastCheck ? lastCheck.toISOString() : null,
        error: lastError
    };
}

function setLive(status) {
    if (status === isLive) return;
    isLive = status;
    console.log(`[STATUS] @${username} → ${status ? "🟢 LIVE" : "🔴 OFFLINE"}`);
}

// VERIFICAÇÃO
async function checkLive() {
    if (checking) return;
    checking = true;

    try {
        const live = await client.fetchIsLive();
        lastError = null;

        if (live) {
            offlineStreak = 0;
            setLive(true);
        } else {
            offlineStreak++;
            if (offlineStreak >= OFFLINE_CONFIRMATIONS || !isLive) setLive(false);
        }

        console.log(`[CHECK] @${username}: ${live ? "online" : "offline"}`);
    } catch (err) {
        // Falha na consulta NÃO muda o status (evita falso offline por erro de rede/bloqueio)
        lastError = err?.message || String(err);
        const causes = err?.requestErrs?.map(e => e?.message).filter(Boolean);
        console.error("❌ Falha ao verificar live:", lastError, causes?.length ? causes : "");
    } finally {
        lastCheck = new Date();
        checking = false;
        io.emit("liveStatus", payload());
    }
}

// SOCKET
io.on("connection", (socket) => {
    socket.emit("liveStatus", payload());
});

// API
app.get("/health", (req, res) => {
    res.json({ live: isLive, lastCheck, error: lastError });
});

app.get("/status", (req, res) => {
    res.json(payload());
});

app.use(express.static("public"));

// START
server.listen(PORT, () => {
    console.log(`🚀 http://localhost:${PORT}`);
    console.log(`Monitorando @${username} a cada ${CHECK_INTERVAL / 1000}s`);
    checkLive();
    setInterval(checkLive, CHECK_INTERVAL);
});
