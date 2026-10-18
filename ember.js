#!/usr/bin/env node
// ember.js — a command-line client for any ember (see README.md). Node built-ins only.
//
//   node ember.js get    <url>            print the ember's current state (the shelf's value) as JSON, then exit
//   node ember.js watch  <url>            one line per change (status · progress · new tells) until it ends or ^C
//   node ember.js stop|pause|resume <url> POST /control
//
// <url> is the ember's base URL; a key can ride along as #k=… or ?k=… (or set EMBER_KEY).
// --insecure (or EMBER_INSECURE=1) accepts a self-signed certificate, e.g. the README's localhost one.
'use strict';
const http2 = require('http2');
const shelf_merge = require('./shelf.js');

const args = process.argv.slice(2).filter((a) => a !== '--insecure');
const insecure = process.argv.includes('--insecure') || process.env.EMBER_INSECURE === '1';
const [cmd, raw] = args;
if (!cmd || !raw) { console.log(require('fs').readFileSync(__filename, 'utf8').split('\n').filter((l) => l.startsWith('//')).map((l) => l.slice(3)).join('\n')); process.exit(2); }

const u = new URL(raw);
const key = new URLSearchParams(u.hash.slice(1)).get('k') || u.searchParams.get('k') || process.env.EMBER_KEY || '';
const base = (u.pathname.endsWith('/') ? u.pathname : u.pathname + '/');
const headers = (extra) => Object.assign(key ? { authorization: 'Bearer ' + key } : {}, extra);
const die = (msg) => { console.error('ember: ' + msg); process.exit(1); };

const client = http2.connect(u.origin, { rejectUnauthorized: !insecure });
client.on('error', (e) => die(e.message));

if (['stop', 'pause', 'resume'].includes(cmd)) {
  const req = client.request(headers({ ':method': 'POST', ':path': base + 'control', 'content-type': 'application/json' }));
  let out = '';
  req.on('data', (d) => (out += d));
  req.on('end', () => { console.log(out); client.close(); });
  req.end(JSON.stringify({ action: cmd }));
} else if (cmd === 'get' || cmd === 'watch') {
  const req = client.request(headers({ ':path': base + 'state', accept: 'text/event-stream' }));
  req.on('response', (h) => { if (h[':status'] !== 200) die('GET state → ' + h[':status']); });
  const doc = [null, null];
  let buf = '', lastLine = '', lastTell = 0;
  const line = (s) => {
    const p = s.progress || {};
    const stalled = (s.status === 'running' || s.status === 'paused') && Date.now() - (s.beat || 0) > 90000;
    return `${s.emoji || '🔥'} ${s.title} · ${stalled ? 'stalled' : s.status}${p.total ? ` · ${p.done}/${p.total}` : ''}${p.msg ? ' · ' + p.msg : ''}${s.error ? ' · ' + s.error : ''}`;
  };
  req.setEncoding('utf8');
  req.on('data', (d) => {
    buf += d;
    let i;
    while ((i = buf.indexOf('\n\n')) >= 0) {
      const block = buf.slice(0, i); buf = buf.slice(i + 2);
      let event = 'message', data = '';
      for (const l of block.split('\n')) {
        if (l.startsWith('event:')) event = l.slice(6).trim();
        else if (l.startsWith('data:')) data += l.slice(5).trim();
      }
      if (event !== 'shelf' && event !== 'delta') continue;   // comments (": ping") have no event
      if (event === 'shelf') { doc[0] = null; doc[1] = null; }
      shelf_merge(doc, JSON.parse(data));
      const s = doc[0];
      if (cmd === 'get') { console.log(JSON.stringify(s, null, 1)); req.close(); client.close(); return; }
      const l = line(s);
      if (l !== lastLine) { console.log(new Date().toISOString().slice(11, 19), l); lastLine = l; }
      for (const m of s.tell || []) if (m.t > lastTell) { console.log('   💬', m.text); lastTell = m.t; }
      if (['done', 'failed', 'stopped'].includes(s.status)) { req.close(); client.close(); return; }
    }
  });
  req.on('end', () => client.close());
} else die('unknown command: ' + cmd);
