#!/usr/bin/env node
// example.js — a minimal ember (see README.md). The "work" just counts to EMBER_TOTAL, one step every
// EMBER_STEP_MS; replace tick() with something real. Everything else is the contract:
//
//   GET  /          the dashboard (needs no key; it reads #k=<key> and passes it on)
//   GET  /state     SSE: event "shelf" (the full shelf) first, then an event "delta" per change; ": ping" every 15 s
//   POST /control   {"action":"stop"|"pause"|"resume"} → {ok, status} or {ok:false, error}
//
// Served over HTTP/2 + TLS (HTTP/1.1 also accepted). Node built-ins only.
// Env: EMBER_PORT (63200) · EMBER_TLS_CERT / EMBER_TLS_KEY (default ./cert.pem, ./key.pem) · EMBER_KEY (unset = open)
//      EMBER_TITLE · EMBER_TOTAL (20) · EMBER_STEP_MS (1000) · EMBER_OWNER_TOWER + EMBER_OWNER_PLANE (who to tell)
'use strict';
const fs = require('fs'), path = require('path'), http2 = require('http2');
const shelf_merge = require('./shelf.js');

const PORT = +(process.env.EMBER_PORT || 63200);
const CERT = process.env.EMBER_TLS_CERT || path.join(__dirname, 'cert.pem');
const TKEY = process.env.EMBER_TLS_KEY || path.join(__dirname, 'key.pem');
const KEY = process.env.EMBER_KEY || '';
const TITLE = process.env.EMBER_TITLE || 'example ember';
const TOTAL = +(process.env.EMBER_TOTAL || 20), STEP_MS = +(process.env.EMBER_STEP_MS || 1000);
const TERMINAL = new Set(['done', 'failed', 'stopped']);

// ---------- the shelf: this process is its only writer; every change goes to every subscriber as a delta ----------
const shelf = [null, null];
const subs = new Set();
const sse = (res, event, data) => res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
function set(patch) {
  const delta = shelf_merge(shelf, [patch, null]);
  if (delta) for (const res of subs) sse(res, 'delta', delta);
}
const S = () => shelf[0];
// tell: the last 3 messages for the owner plane, newest last (an array, so it's replaced whole)
const tell = (text) => set({ tell: [...(S().tell || []), { t: Date.now(), text }].slice(-3) });

const owner = process.env.EMBER_OWNER_TOWER && process.env.EMBER_OWNER_PLANE
  ? { tower: process.env.EMBER_OWNER_TOWER, plane: process.env.EMBER_OWNER_PLANE } : null;
set({ ember: 1, title: TITLE, emoji: '🔥', status: 'running', progress: { done: 0, total: TOTAL, msg: 'starting' },
  started: Date.now(), beat: Date.now(), controls: { stop: true, pause: true, resume: true }, owner, tell: [] });

// ---------- the work ----------
function tick() {
  if (S().status !== 'running') return;
  const done = S().progress.done + 1;
  set({ progress: { done, msg: `step ${done} of ${TOTAL}` } });
  if (done >= TOTAL) finish('done');
}
const work = setInterval(tick, STEP_MS);
const beat = setInterval(() => set({ beat: Date.now() }), 10000);   // liveness: ≤ 30 s apart while not finished
function finish(status, error) {
  clearInterval(work); clearInterval(beat);
  set({ status, ended: Date.now(), error: error || null });
  tell(`${TITLE}: ${status} · ${S().progress.done}/${TOTAL}${error ? ' · ' + error : ''}`);
}
function control(action) {
  const st = S().status;
  if (TERMINAL.has(st)) return { ok: false, error: 'already ' + st };
  if (action === 'stop') finish('stopped');
  else if (action === 'pause' && st === 'running') set({ status: 'paused' });
  else if (action === 'resume' && st === 'paused') set({ status: 'running' });
  else return { ok: false, error: `can't ${action} while ${st}` };
  return { ok: true, status: S().status };
}

// ---------- http/2 ----------
const PAGE = fs.readFileSync(path.join(__dirname, 'example.html'), 'utf8')
  .replace('/*SHELF*/', () => fs.readFileSync(path.join(__dirname, 'shelf.js'), 'utf8'));
const json = (res, code, o) => { res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(o)); };
const server = http2.createSecureServer({ cert: fs.readFileSync(CERT), key: fs.readFileSync(TKEY), allowHTTP1: true }, (req, res) => {
  const url = new URL(req.url, 'https://x');
  if (req.method === 'GET' && url.pathname === '/') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
    return res.end(PAGE);
  }
  if (KEY && url.searchParams.get('k') !== KEY && req.headers.authorization !== 'Bearer ' + KEY) return json(res, 401, { ok: false, error: 'key' });
  if (req.method === 'GET' && url.pathname === '/state') {
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' });
    sse(res, 'shelf', shelf);
    subs.add(res);
    res.on('close', () => subs.delete(res));
    return;
  }
  if (req.method === 'POST' && url.pathname === '/control') {
    let body = '';
    req.on('data', (d) => { body += d; if (body.length > 1e5) req.destroy(); });
    req.on('end', () => {
      let action = ''; try { action = String(JSON.parse(body || '{}').action || ''); } catch (_) {}
      const r = control(action);
      json(res, r.ok ? 200 : 409, r);
    });
    return;
  }
  json(res, 404, { ok: false, error: 'not found' });
});
setInterval(() => { for (const res of subs) res.write(': ping\n\n'); }, 15000);
server.listen(PORT, () => console.log(`🔥 ${TITLE} on https://localhost:${PORT}/${KEY ? '#k=' + KEY : ''}`));
