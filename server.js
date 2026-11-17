const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');
const { createClient } = require('@supabase/supabase-js');

const { SUPABASE_URL, SUPABASE_SERVICE_KEY } = process.env;
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_KEY environment variable.');
  process.exit(1);
}
const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, { auth: { persistSession: false } });
const BUCKET = 'chat-media';
const GIPHY_KEY = process.env.GIPHY_API_KEY || '';
const GIPHY_RATING = process.env.GIPHY_RATING || 'g'; // g, pg, pg-13 or r
const isUrl = (p) => typeof p === 'string' && p.startsWith('https://');
const GIF_HOST = /^https:\/\/media[0-9]*\.giphy\.com\//;
const URL_TTL = 6 * 3600; // signed media links last 6 hours

const app = express();
app.set('trust proxy', 1);
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 25e6 });

// The two 4-digit codes. Set CODE_A and CODE_B as environment variables.
const CODES = { [process.env.CODE_A || '1234']: 'A', [process.env.CODE_B || '5678']: 'B' };
const fails = new Map();

app.use((req, res, next) => { res.set('X-Robots-Tag', 'noindex'); next(); });
app.use(express.static('public'));

io.use((socket, next) => {
  const ip = socket.handshake.headers['x-forwarded-for']?.split(',')[0] || socket.handshake.address;
  const f = fails.get(ip) || { n: 0, until: 0 };
  if (Date.now() < f.until) return next(new Error('denied'));
  const user = CODES[String(socket.handshake.auth.code)];
  if (!user) {
    f.n++;
    if (f.n >= 10) { f.until = Date.now() + 15 * 60 * 1000; f.n = 0; }
    fails.set(ip, f);
    return next(new Error('denied'));
  }
  fails.delete(ip);
  socket.user = user;
  socket.join(user);
  next();
});

const online = (u) => (io.sockets.adapter.rooms.get(u)?.size || 0) > 0;

function toMsg(r, url) {
  const m = { id: r.id, from: r.sender, t: new Date(r.created_at).getTime(), text: r.text };
  if (r.media_path) m.media = { name: r.media_name, type: r.media_type, url: isUrl(r.media_path) ? r.media_path : url };
  return m;
}

async function loadMessages() {
  const { data, error } = await sb.from('messages').select('*').order('created_at', { ascending: false }).limit(300);
  if (error) throw error;
  const rows = data.reverse();
  const paths = rows.filter((r) => r.media_path && !isUrl(r.media_path)).map((r) => r.media_path);
  const urls = {};
  if (paths.length) {
    const { data: s } = await sb.storage.from(BUCKET).createSignedUrls(paths, URL_TTL);
    (s || []).forEach((x) => { if (x.signedUrl) urls[x.path] = x.signedUrl; });
  }
  return rows.map((r) => toMsg(r, urls[r.media_path]));
}

io.on('connection', async (socket) => {
  const me = socket.user, other = me === 'A' ? 'B' : 'A';

  socket.on('msg', async (m) => {
    try {
      const text = String(m?.text || '').slice(0, 4000);
      let media = null;
      const d = m?.media?.data;
      if (typeof d === 'string' && /^data:[^;,]*;base64,/.test(d)) {
        const buf = Buffer.from(d.split(',')[1], 'base64');
        if (buf.length > 20e6) return socket.emit('err', 'File too large');
        const type = String(m.media.type || 'application/octet-stream').slice(0, 100);
        const name = String(m.media.name || 'file').slice(0, 120);
        const safe = name.replace(/[^\w.-]/g, '_').slice(-60);
        const path = `${Date.now()}-${crypto.randomUUID()}-${safe}`;
        const { error } = await sb.storage.from(BUCKET).upload(path, buf, { contentType: type });
        if (error) throw error;
        media = { path, name, type };
      }
      if (!media && typeof m?.gif === 'string' && GIF_HOST.test(m.gif) && m.gif.length < 600) {
        media = { path: m.gif, name: 'GIF', type: 'image/gif' }; // stored as a link, not a file
      }
      if (!text && !media) return;
      const { data, error } = await sb.from('messages')
        .insert({ sender: me, text, media_path: media?.path, media_name: media?.name, media_type: media?.type })
        .select().single();
      if (error) throw error;
      let url;
      if (media && !isUrl(media.path)) url = (await sb.storage.from(BUCKET).createSignedUrl(media.path, URL_TTL)).data?.signedUrl;
      io.to('A').to('B').emit('msg', toMsg(data, url));
    } catch (e) { console.error(e); socket.emit('err', 'Message failed to send'); }
  });

  socket.on('del', async (id) => { // only your own messages; also removes the media file
    try {
      const { data, error } = await sb.from('messages').delete().eq('id', id).eq('sender', me).select();
      if (error || !data?.length) return;
      if (data[0].media_path && !isUrl(data[0].media_path)) await sb.storage.from(BUCKET).remove([data[0].media_path]);
      io.to('A').to('B').emit('del', id);
    } catch (e) { console.error(e); }
  });

  socket.on('clear', async () => {
    try {
      const { data } = await sb.from('messages').select('media_path').not('media_path', 'is', null);
      const paths = (data || []).map((r) => r.media_path).filter((p) => !isUrl(p));
      for (let i = 0; i < paths.length; i += 100) await sb.storage.from(BUCKET).remove(paths.slice(i, i + 100));
      const { error } = await sb.from('messages').delete().not('id', 'is', null);
      if (error) throw error;
      io.to('A').to('B').emit('clear');
    } catch (e) { console.error(e); socket.emit('err', 'Could not clear chat'); }
  });

  socket.on('gifs', async (q, cb) => { // GIF search, proxied so the Giphy key stays on the server
    if (typeof cb !== 'function') return;
    if (!GIPHY_KEY) return cb([]);
    try {
      const term = String(q || '').trim().slice(0, 60);
      const u = new URL('https://api.giphy.com/v1/gifs/' + (term ? 'search' : 'trending'));
      u.searchParams.set('api_key', GIPHY_KEY);
      u.searchParams.set('limit', '24');
      u.searchParams.set('rating', GIPHY_RATING);
      if (term) u.searchParams.set('q', term);
      const j = await (await fetch(u)).json();
      cb((j.data || []).map((g) => ({
        id: g.id,
        preview: g.images?.fixed_width_small?.url || g.images?.fixed_width?.url,
        url: g.images?.downsized?.url || g.images?.original?.url
      })).filter((g) => GIF_HOST.test(g.preview || '') && GIF_HOST.test(g.url || '')));
    } catch (e) { console.error(e); cb([]); }
  });

  socket.on('sig', (d) => io.to(other).emit('sig', d)); // WebRTC signalling
  socket.on('disconnect', () => { if (!online(me)) io.to(other).emit('presence', false); });

  try {
    socket.emit('init', { me, messages: await loadMessages(), presence: online(other), gifs: !!GIPHY_KEY });
    io.to(other).emit('presence', true);
  } catch (e) { console.error(e); socket.emit('err', 'Could not load history'); }
});

// Light query twice a day so the free Supabase project never counts as inactive
setInterval(() => { sb.from('messages').select('id').limit(1).then(() => {}, () => {}); }, 12 * 3600 * 1000);

server.listen(process.env.PORT || 3000, () => console.log('Running on port ' + (process.env.PORT || 3000)));
