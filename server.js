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

// db estructura: { [userId]: { [projectId]: { id, name, data, savedAt } } }
let projectsDB = loadProjectsDB();

// ── Rooms ─────────────────────────────────────────────────────────────────────
const rooms = {};

io.on('connection', (socket) => {
  console.log('🔌 Conectado:', socket.id);

  // ── Sesiones ──────────────────────────────────────────────────────────────

  socket.on('create-session', ({ code, name }) => {
    rooms[code] = { host: socket.id, peers: [{ id: socket.id, name, role: 'Host' }] };
    socket.join(code);
    socket.sessionCode = code;
    socket.emit('session-created', { code, peers: rooms[code].peers });
    console.log(`🎬 Sesión creada: ${code} por ${name}`);
  });

  socket.on('join-session', ({ code, name }) => {
    if (!rooms[code]) { socket.emit('error-msg', 'Sesión no encontrada.'); return; }
    const peer = { id: socket.id, name, role: 'Co-Director' };
    rooms[code].peers.push(peer);
    socket.join(code);
    socket.sessionCode = code;
    io.to(code).emit('peers-updated', rooms[code].peers);
    socket.emit('session-joined', { code, peers: rooms[code].peers });
    console.log(`👥 ${name} se unió a: ${code}`);
  });

  // ── Perfil de peer ─────────────────────────────────────────────────────────
  // Cliente envía: { profile: { avatar, color, … } }
  // Server actualiza el peer en rooms y hace broadcast al room
  socket.on('peer-profile', (profile) => {
    const code = socket.sessionCode;
    if (!code || !rooms[code]) return;
    const peer = rooms[code].peers.find(p => p.id === socket.id);
    if (peer) Object.assign(peer, { profile });
    io.to(code).emit('peer-profile', { id: socket.id, profile });
    console.log(`👤 Perfil actualizado: ${socket.id}`);
  });

  // ── Proyecto: relay host → guests ──────────────────────────────────────────

  socket.on('project-load', (data) => {
    const code = socket.sessionCode;
    if (!code) return;
    socket.to(code).emit('project-load', data);
  });

  socket.on('project-update', (data) => {
    const code = socket.sessionCode;
    if (!code) return;
    socket.to(code).emit('project-update', data);
  });

  socket.on('cursor-move', (data) => {
    const code = socket.sessionCode;
    if (!code) return;
    socket.to(code).emit('cursor-move', { ...data, id: socket.id });
  });

  // ── Proyectos en servidor ──────────────────────────────────────────────────
  // Todos los eventos reciben { reqId, userId, … } y responden con
  // socket.emit('srv-response', { reqId, ok, data })

  // Guardar proyecto
  // Payload: { reqId, userId, projectId?, data }
  // data.projectName se usa como nombre visible
  socket.on('srv-save', ({ reqId, userId, projectId, name, data }) => {
    try {
      if (!userId) throw new Error('userId requerido');
      if (!projectsDB[userId]) projectsDB[userId] = {};
      // El cliente manda el nombre dentro de data.projectName
      const projectName = (data && data.projectName) || name || 'Sin título';
      // Buscar proyecto existente por nombre para sobrescribir en vez de duplicar
      const existingId = projectId || Object.keys(projectsDB[userId] || {})
        .find(k => projectsDB[userId][k].projectName === projectName);
      const id = existingId || `proj_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
      const savedAt = new Date().toISOString();
      projectsDB[userId][id] = { id, projectName, data, savedAt };
      saveProjectsDB(projectsDB);
      socket.emit('srv-response', { reqId, ok: true, data: { id, savedAt } });
      console.log(`💾 Proyecto guardado: "${projectName}" id=${id} (user: ${userId})`);
    } catch (e) {
      socket.emit('srv-response', { reqId, ok: false, data: { error: e.message } });
    }
  });

  // Listar proyectos del usuario
  // Payload: { reqId, userId }
  // Responde con el ARRAY directamente (el cliente hace projects.length)
  socket.on('srv-list', ({ reqId, userId }) => {
    try {
      if (!userId) throw new Error('userId requerido');
      const userProjects = projectsDB[userId] || {};
      const list = Object.values(userProjects)
        .map(({ id, projectName, savedAt }) => ({ id, projectName, savedAt }));
      list.sort((a, b) => new Date(b.savedAt) - new Date(a.savedAt));
      // El cliente espera el array directamente, no envuelto en { projects: [...] }
      socket.emit('srv-response', { reqId, ok: true, data: list });
    } catch (e) {
      socket.emit('srv-response', { reqId, ok: false, data: { error: e.message } });
    }
  });

  // Cargar proyecto por ID
  // Payload: { reqId, userId, id }   (el cliente manda "id", no "projectId")
  socket.on('srv-load', ({ reqId, userId, id, projectId }) => {
    try {
      const pid = id || projectId;
      if (!userId || !pid) throw new Error('userId y id requeridos');
      const entry = projectsDB[userId]?.[pid];
      if (!entry) throw new Error('Proyecto no encontrado');
      // El cliente usa data.projectName, data.objects, etc. directamente
      socket.emit('srv-response', { reqId, ok: true, data: entry.data });
      console.log(`📂 Proyecto cargado: ${pid} (user: ${userId})`);
    } catch (e) {
      socket.emit('srv-response', { reqId, ok: false, data: { error: e.message } });
    }
  });

  // Borrar proyectos
  // Payload: { reqId, userId, projectId? }  — sin projectId borra todos
  socket.on('srv-clear', ({ reqId, userId, projectId }) => {
    try {
      if (!userId) throw new Error('userId requerido');
      if (projectId) {
        if (!projectsDB[userId]?.[projectId]) throw new Error('Proyecto no encontrado');
        delete projectsDB[userId][projectId];
        console.log(`🗑 Proyecto borrado: ${projectId} (user: ${userId})`);
      } else {
        delete projectsDB[userId];
        console.log(`🗑 Todos los proyectos borrados (user: ${userId})`);
      }
      saveProjectsDB(projectsDB);
      socket.emit('srv-response', { reqId, ok: true, data: { cleared: projectId || 'all' } });
    } catch (e) {
      socket.emit('srv-response', { reqId, ok: false, data: { error: e.message } });
    }
  });

  // ── Desconexión ────────────────────────────────────────────────────────────

  socket.on('disconnect', () => {
    const code = socket.sessionCode;
    if (!code || !rooms[code]) return;
    rooms[code].peers = rooms[code].peers.filter(p => p.id !== socket.id);
    if (rooms[code].peers.length === 0) { delete rooms[code]; console.log(`🗑 Sesión cerrada: ${code}`); }
    else io.to(code).emit('peers-updated', rooms[code].peers);
    console.log('❌ Desconectado:', socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Servidor corriendo en puerto ${PORT}`));
