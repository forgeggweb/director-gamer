const express  = require('express');
const http     = require('http');
const { Server } = require('socket.io');
const path     = require('path');
const fs       = require('fs');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
  cors: { origin: '*', methods: ['GET','POST'] }
});

app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ── Persistencia de proyectos ─────────────────────────────────────────────────
const PROJECTS_FILE = path.join(__dirname, 'projects.json');

function loadProjectsDB() {
  try {
    if (fs.existsSync(PROJECTS_FILE)) {
      return JSON.parse(fs.readFileSync(PROJECTS_FILE, 'utf8'));
    }
  } catch (e) { console.error('Error leyendo projects.json:', e.message); }
  return {};
}

function saveProjectsDB(db) {
  try {
    fs.writeFileSync(PROJECTS_FILE, JSON.stringify(db, null, 2), 'utf8');
  } catch (e) { console.error('Error guardando projects.json:', e.message); }
}

// db estructura: { [userId]: { [projectId]: { id, projectName, data, savedAt } } }
let projectsDB = loadProjectsDB();

// ── Rooms ─────────────────────────────────────────────────────────────────────
const rooms = {};

io.on('connection', (socket) => {
  console.log('🔌 Conectado:', socket.id);

  // ── Sesiones ──────────────────────────────────────────────────────────────

  socket.on('create-session', ({ code, name, profile }) => {
    rooms[code] = {
      host: socket.id,
      peers: [{ id: socket.id, name, role: 'Host', profile: profile || null }]
    };
    socket.join(code);
    socket.sessionCode = code;
    socket.emit('session-created', { code, peers: rooms[code].peers });
    console.log(`🎬 Sesión creada: ${code} por ${name}`);
  });

  socket.on('join-session', ({ code, name, profile }) => {
    if (!rooms[code]) { socket.emit('error-msg', 'Sesión no encontrada.'); return; }
    const peer = { id: socket.id, name, role: 'Co-Director', profile: profile || null };
    rooms[code].peers.push(peer);
    socket.join(code);
    socket.sessionCode = code;
    // Primero notificar a todos que hay un nuevo peer
    io.to(code).emit('peers-updated', rooms[code].peers);
    // Luego confirmar al que se unió (incluye la lista completa de peers)
    socket.emit('session-joined', { code, peers: rooms[code].peers });
    console.log(`👥 ${name} se unió a: ${code}`);
  });

  // ── Perfil de peer ─────────────────────────────────────────────────────────
  // FIX: el cliente envía { sessionCode, profile, name } — desestructurar correctamente
  socket.on('peer-profile', ({ sessionCode, profile, name }) => {
    const code = sessionCode || socket.sessionCode;
    if (!code || !rooms[code]) return;
    const peer = rooms[code].peers.find(p => p.id === socket.id);
    if (peer) {
      if (profile) peer.profile = profile;
      if (name)    peer.name    = name;
    }
    // Reenviar solo a los demás (fromId para que el cliente ignore el eco propio)
    socket.to(code).emit('peer-profile', { fromId: socket.id, profile, name });
    console.log(`👤 Perfil actualizado: ${socket.id} (${name || ''})`);
  });

  // ── Proyecto: relay ────────────────────────────────────────────────────────

  socket.on('project-load', (data) => {
    const code = socket.sessionCode;
    if (!code) return;
    // Solo el host puede enviar project-load
    if (rooms[code]?.host !== socket.id) return;
    socket.to(code).emit('project-load', data);
  });

  socket.on('project-update', (data) => {
    const code = socket.sessionCode;
    if (!code) return;
    // Relay a todos menos el emisor (socket.to ya excluye al emisor)
    socket.to(code).emit('project-update', data);
  });

  socket.on('cursor-move', (data) => {
    const code = socket.sessionCode;
    if (!code) return;
    socket.to(code).emit('cursor-move', { ...data, id: socket.id });
  });

  // ── Proyectos en servidor ──────────────────────────────────────────────────
  // Todos los eventos reciben { reqId, userId, … } y responden con
  // socket.emit('srv-response', { reqId, ok, data, error })

  // FIX: error se manda en el campo "error", no dentro de "data"
  function srvOk(reqId, data)    { socket.emit('srv-response', { reqId, ok: true,  data }); }
  function srvErr(reqId, msg)    { socket.emit('srv-response', { reqId, ok: false, error: msg }); }

  socket.on('srv-save', ({ reqId, userId, projectId, name, data }) => {
    try {
      if (!userId) throw new Error('userId requerido');
      if (!projectsDB[userId]) projectsDB[userId] = {};
      const projectName = (data && data.projectName) || name || 'Sin título';
      const existingId = projectId || Object.keys(projectsDB[userId])
        .find(k => projectsDB[userId][k].projectName === projectName);
      const id = existingId || `proj_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
      const savedAt = new Date().toISOString();
      projectsDB[userId][id] = { id, projectName, data, savedAt };
      saveProjectsDB(projectsDB);
      srvOk(reqId, { id, savedAt });
      console.log(`💾 Guardado: "${projectName}" id=${id} (user: ${userId})`);
    } catch (e) { srvErr(reqId, e.message); }
  });

  socket.on('srv-list', ({ reqId, userId }) => {
    try {
      if (!userId) throw new Error('userId requerido');
      const list = Object.values(projectsDB[userId] || {})
        .map(({ id, projectName, savedAt }) => ({ id, projectName, savedAt }))
        .sort((a, b) => new Date(b.savedAt) - new Date(a.savedAt));
      srvOk(reqId, list);
    } catch (e) { srvErr(reqId, e.message); }
  });

  socket.on('srv-load', ({ reqId, userId, id, projectId }) => {
    try {
      const pid = id || projectId;
      if (!userId || !pid) throw new Error('userId y id requeridos');
      const entry = projectsDB[userId]?.[pid];
      if (!entry) throw new Error('Proyecto no encontrado');
      srvOk(reqId, entry.data);
      console.log(`📂 Cargado: ${pid} (user: ${userId})`);
    } catch (e) { srvErr(reqId, e.message); }
  });

  socket.on('srv-clear', ({ reqId, userId, projectId }) => {
    try {
      if (!userId) throw new Error('userId requerido');
      if (projectId) {
        if (!projectsDB[userId]?.[projectId]) throw new Error('Proyecto no encontrado');
        delete projectsDB[userId][projectId];
        console.log(`🗑 Borrado: ${projectId} (user: ${userId})`);
      } else {
        delete projectsDB[userId];
        console.log(`🗑 Todos borrados (user: ${userId})`);
      }
      saveProjectsDB(projectsDB);
      srvOk(reqId, { cleared: projectId || 'all' });
    } catch (e) { srvErr(reqId, e.message); }
  });

  // ── Desconexión ────────────────────────────────────────────────────────────

  socket.on('disconnect', () => {
    const code = socket.sessionCode;
    if (!code || !rooms[code]) return;
    rooms[code].peers = rooms[code].peers.filter(p => p.id !== socket.id);
    if (rooms[code].peers.length === 0) {
      delete rooms[code];
      console.log(`🗑 Sesión cerrada: ${code}`);
    } else {
      io.to(code).emit('peers-updated', rooms[code].peers);
    }
    console.log('❌ Desconectado:', socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Servidor corriendo en puerto ${PORT}`));
