const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');

const app = express();
app.set('trust proxy', 1);
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 25e6 }); // ~25 MB per message

// The two 4-digit codes. Set CODE_A and CODE_B as environment variables when hosting.
const CODES = { [process.env.CODE_A || '1234']: 'A', [process.env.CODE_B || '5678']: 'B' };

let messages = []; // kept in memory; restarting the server clears the chat
const fails = new Map(); // ip -> { n, until }

app.use((req, res, next) => { res.set('X-Robots-Tag', 'noindex'); next(); });
app.use(express.static('public'));

io.use((socket, next) => {
  const ip = socket.handshake.headers['x-forwarded-for']?.split(',')[0] || socket.handshake.address;
  const f = fails.get(ip) || { n: 0, until: 0 };
  if (Date.now() < f.until) return next(new Error('denied'));
  const user = CODES[String(socket.handshake.auth.code)];
  if (!user) {
    f.n++;
    if (f.n >= 10) { f.until = Date.now() + 15 * 60 * 1000; f.n = 0; } // lock out 15 min after 10 misses
    fails.set(ip, f);
    return next(new Error('denied'));
  }
  fails.delete(ip);
  socket.user = user;
  socket.join(user);
  next();
});

const online = (u) => (io.sockets.adapter.rooms.get(u)?.size || 0) > 0;

io.on('connection', (socket) => {
  const me = socket.user, other = me === 'A' ? 'B' : 'A';
  socket.emit('init', { me, messages, presence: online(other) });
  io.to(other).emit('presence', true);

  socket.on('msg', (m) => {
    const msg = { id: crypto.randomUUID(), from: me, t: Date.now(), text: String(m?.text || '').slice(0, 4000) };
    if (m?.media?.data && String(m.media.data).startsWith('data:')) {
      msg.media = { name: String(m.media.name || 'file').slice(0, 120), type: String(m.media.type || ''), data: m.media.data };
    }
    if (!msg.text && !msg.media) return;
    messages.push(msg);
    io.to('A').to('B').emit('msg', msg);
  });

  socket.on('del', (id) => { // only your own messages
    const i = messages.findIndex((x) => x.id === id && x.from === me);
    if (i < 0) return;
    messages.splice(i, 1);
    io.to('A').to('B').emit('del', id);
  });

  socket.on('clear', () => { messages = []; io.to('A').to('B').emit('clear'); });

  socket.on('sig', (d) => io.to(other).emit('sig', d)); // WebRTC signalling relay

  socket.on('disconnect', () => { if (!online(me)) io.to(other).emit('presence', false); });
});

server.listen(process.env.PORT || 3000, () => console.log('Running on port ' + (process.env.PORT || 3000)));
