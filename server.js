const express  = require('express');
const http     = require('http');
const { Server } = require('socket.io');
const path     = require('path');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
  cors: { origin: '*', methods: ['GET','POST'] }
});

app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const rooms = {};

io.on('connection', (socket) => {
  console.log('🔌 Conectado:', socket.id);

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

  // Host envía el proyecto inicial — se reenvía solo a los guests
  socket.on('project-load', (data) => {
    const code = socket.sessionCode;
    if (!code) return;
    socket.to(code).emit('project-load', data);
  });

  // Host broadcast cambios
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
