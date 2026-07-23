import express from "express";
import http from "http";
import { Server } from "socket.io";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const { TikTokLiveConnection, ControlEvent, ControlAction } = require("tiktok-live-connector");

const app = express();
const server = http.createServer(app);

// CORS seguro (inclui localhost por padrão, além do que vier em ALLOWED_ORIGINS)
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
                console.log(`⚠️ CORS bloqueou origem: ${origin}`);
                callback(new Error("Not allowed by CORS"));
            }
        },
        methods: ["GET", "POST"]
    }
});

// Config
// Sem fallback silencioso: se TIKTOK_USER não estiver configurado, o
// servidor recusa a subir em vez de monitorar uma conta errada sem avisar.
const username = process.env.TIKTOK_USER;
if (!username) {
    console.error("❌ TIKTOK_USER não configurado. Defina a variável de ambiente antes de iniciar.");
    process.exit(1);
}

const PORT = process.env.PORT || 3000;

const RETRY_BASE = 30000;
const RETRY_ERROR = 60000;
const MAX_BACKOFF = 5 * 60 * 1000;

const HEARTBEAT_INTERVAL = 60000;
const HEARTBEAT_FAIL_LIMIT = 3;
const HEARTBEAT_GRACE_PERIOD = 90000;
const HEARTBEAT_MAX_DELAY = 120000;
const ACTIVITY_WINDOW = 180000; // 3 min

// Estado
let isLive = false;
let connection = null;
let retryTimer = null;
let heartbeatTimer = null;
let isConnecting = false;
let retryCount = 0;
let retryScheduled = false;
let lastCheck = null;
let connectedAt = null;
let lastLiveChange = 0;

// Controle de atividade real (chat/like/gift/member).
// Começa em 0 (nunca houve atividade) — só passa a ter valor quando um
// evento real chega. Isso evita que os primeiros minutos após conectar
// sejam tratados como "atividade recente" apenas por a conexão ser nova.
let lastActivity = 0;

// Atualiza status
function setLive(status) {
    const now = Date.now();

    if (!status && status !== isLive && now - lastLiveChange < 2000) return;

    lastCheck = new Date().toISOString();

    if (status !== isLive) {
        isLive = status;
        lastLiveChange = now;
        console.log(`[${lastCheck}] ${username} →`, isLive ? "🟢 AO VIVO" : "🔴 OFFLINE");
    }

    io.emit("liveStatus", { user: username, online: isLive, lastCheck });
}

// Reset geral
function resetState() {
    clearTimeout(retryTimer);
    clearInterval(heartbeatTimer);
    retryTimer = null;
    heartbeatTimer = null;
    retryScheduled = false; // evita travar scheduleRetry() se resetState() for
    // chamado enquanto um retry já estava agendado
}

// Retry
function scheduleRetry(baseDelay) {
    if (retryScheduled) return;

    retryScheduled = true;

    const delay = Math.min(baseDelay * Math.pow(2, retryCount), MAX_BACKOFF);
    retryCount++;

    console.log(`🔁 Tentando novamente em ${delay / 1000}s`);

    retryTimer = setTimeout(() => {
        retryScheduled = false;
        connectToLive();
    }, delay);
}

// Cleanup
function cleanupConnection() {
    clearInterval(heartbeatTimer);

    if (connection) {
        connection.removeAllListeners();
        try { connection.disconnect(); } catch (_) { }
        connection = null;
    }
}

// Heartbeat
function startHeartbeat() {
    clearInterval(heartbeatTimer);

    let heartbeatFails = 0;
    let lastHeartbeatOk = Date.now();

    heartbeatTimer = setInterval(async () => {
        if (!connection) return;

        const now = Date.now();
        const timeSinceConnect = now - (connectedAt || now);
        const timeSinceActivity = now - lastActivity;

        // Só considera "atividade recente" se realmente já houve
        // alguma atividade registrada (lastActivity > 0).
        const hasRecentActivity = lastActivity > 0 && timeSinceActivity < ACTIVITY_WINDOW;

        try {
            const roomInfo = await connection.fetchRoomInfo();

            const stillLive = roomInfo?.status === 2 || hasRecentActivity;

            if (stillLive) {
                heartbeatFails = 0;
                lastHeartbeatOk = now;

                console.log(`💓 OK ${hasRecentActivity ? "(atividade)" : "(api)"}`);

                setLive(true);
                return;
            }

            if (timeSinceConnect < HEARTBEAT_GRACE_PERIOD) {
                console.log("⏳ Ignorando OFFLINE (grace period)");
                return;
            }

            console.log("💓 Possível queda");
            heartbeatFails++;

        } catch (err) {
            heartbeatFails++;
            console.log(`💓 Falha heartbeat (${heartbeatFails})`);
        }

        const delaySinceLastOk = now - lastHeartbeatOk;

        console.log({
            heartbeatFails,
            delay: Math.floor(delaySinceLastOk / 1000) + "s",
            semAtividade: Math.floor(timeSinceActivity / 1000) + "s"
        });

        if (
            heartbeatFails >= HEARTBEAT_FAIL_LIMIT &&
            delaySinceLastOk > HEARTBEAT_MAX_DELAY &&
            timeSinceConnect > HEARTBEAT_GRACE_PERIOD &&
            !hasRecentActivity
        ) {
            console.log("💥 Confirmado OFFLINE real");

            setLive(false);
            cleanupConnection();
            scheduleRetry(RETRY_BASE);
        }

    }, HEARTBEAT_INTERVAL);
}

// Conexão
function connectToLive() {
    if (isConnecting || connection) {
        console.log("⚠️ Já conectando/conectado");
        return;
    }

    isConnecting = true;

    resetState();
    cleanupConnection();

    let tentativaResolvida = false;

    connection = new TikTokLiveConnection(username, {
        requestOptions: { timeout: 8000 },
        processInitialData: false
    });

    // Monitor de atividade real
    const markActivity = () => {
        lastActivity = Date.now();
    };

    connection.on("chat", markActivity);
    connection.on("like", markActivity);
    connection.on("gift", markActivity);
    connection.on("member", markActivity);

    const connectTimeout = setTimeout(() => {
        if (isConnecting && !tentativaResolvida) {
            tentativaResolvida = true;
            console.log("⏱️ Timeout ao conectar");
            isConnecting = false;
            cleanupConnection();
            scheduleRetry(RETRY_ERROR);
        }
    }, 15000);

    connection.once(ControlEvent.CONNECTED, (state) => {
        tentativaResolvida = true;
        clearTimeout(connectTimeout);

        isConnecting = false;
        retryCount = 0;
        connectedAt = Date.now();

        // lastActivity NÃO é setado aqui — conectar não é o mesmo que ter
        // atividade real. Fica em 0 até o primeiro chat/like/gift/member
        // chegar de verdade, evitando falso "benefício da dúvida" nos
        // primeiros minutos após uma conexão que já estava offline.

        console.log(`✅ Conectado à sala ${state.roomId}`);
        setLive(true);
        startHeartbeat();
    });

    connection.once(ControlEvent.DISCONNECTED, ({ code, reason }) => {
        console.log(`🔌 Desconectado (${code}) ${reason || ""}`);

        isConnecting = false;
        setLive(false); // o debounce interno do próprio setLive já cobre flapping
        cleanupConnection();
        scheduleRetry(RETRY_BASE);
    });

    connection.on(ControlEvent.STREAM_END, ({ action }) => {
        console.log(
            action === ControlAction.CONTROL_ACTION_STREAM_SUSPENDED
                ? "🚫 Live suspensa"
                : "🔴 Live encerrada"
        );
    });

    connection.on("error", (err) => {
        console.error({
            type: "connection_error",
            message: err?.message,
            raw: err
        });
    });

    connection.connect().catch(err => {
        if (tentativaResolvida) return;

        tentativaResolvida = true;

        clearTimeout(connectTimeout);
        isConnecting = false;

        setLive(false);

        const isRateLimit = /rate.?limit|429/i.test(err.message || "");

        console.log(
            isRateLimit
                ? "⚠️ Rate limit detectado"
                : "❌ Erro ao conectar"
        );

        scheduleRetry(isRateLimit ? RETRY_ERROR * 2 : RETRY_BASE);
    });
}

// Socket
io.on("connection", (socket) => {
    socket.emit("liveStatus", { user: username, online: isLive, lastCheck });
});

// Health
app.get("/health", (req, res) => {
    res.json({
        status: "ok",
        live: isLive,
        connected: !!connection,
        retryCount,
        uptime: connectedAt ? Date.now() - connectedAt : null,
        lastCheck
    });
});

app.use(express.static("public"));

// Start
server.listen(PORT, () => {
    console.log(`🚀 Rodando em http://localhost:${PORT}`);
    console.log(`Monitorando: @${username}`);
    connectToLive();
});

// Shutdown 
function shutdown() {
    console.log("Encerrando...");
    resetState();
    cleanupConnection();
    io.close();
    server.close(() => process.exit(0));
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

process.on("unhandledRejection", (r) => console.error("Unhandled:", r));
process.on("uncaughtException", (e) => console.error("Uncaught:", e));