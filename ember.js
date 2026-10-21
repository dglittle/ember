#!/usr/bin/env node
// ember.js — the command line for embers (github.com/dglittle/ember). Two halves, one file, Node built-ins only:
//
// 1. A CLIENT for ANY ember: give it the ember's URL (a key rides along as #k=…, ?k=… or EMBER_KEY).
//      node ember.js get    <url>                 the ember's current state as JSON, then exit
//      node ember.js watch  <url>                 one line per change (status · progress · new tells) until it ends
//      node ember.js stop|pause|resume|close <url>   POST /control (close: nobody needs it any more; it stops and exits)
//    --insecure (or EMBER_INSECURE=1) accepts a self-signed certificate.
//
// 2. A JOB RUNNER (optional): long-running jobs as directories, all served as embers by one hub. A job is
//    jobs/<id>/ (under EMBER_ROOT, default this directory) and holds everything about itself:
//      job.json      what it is: {id,title,card,budget_usd,concurrency,min_mem_mb,notify,owner,created}  (written by `new`)
//      run.js        the job's logic: module.exports = async (job) => { … }  (uses the job API below)
//      index.html    the job's own page (optional; the generic page.html is served otherwise)
//      control.json  what we WANT: {"want":"run"|"pause"|"cancel"}       ← written only by the CLI / hub
//      state.json    what IS: status, progress, steps, sessions, spent, pid, beat  ← written only by the runner (atomic)
//      events.jsonl  append-only log the page tails (progress, notes, session start/end, errors)
//      sessions/<name>.jsonl   raw stream-json of each claude session the job ran (live-tailable)
//      out/          the job's results
//    One writer per file, so there are no races. The directory IS the truth: a finished or crashed job's page still works.
//
//    Pieces:
//      runner  `ember.js _run <id>`  — detached (own process group); loads run.js with the job API; heartbeat;
//              polls control.json every 1 s (cancel → kill every live session's process group, then stop; pause → no new work).
//      hub     `ember.js hub`        — one HTTP/2 + TLS server (EMBER_PORT, default 63100): / = every job ·
//              /j/<id>/ = the job's page + its files (read-only, key needed) · a small JSON API. Every job is also an
//              ember per the contract: /j/<id>/state (SSE) + /j/<id>/control.
//      tick    `ember.js tick`       — run it from cron every minute: keeps the hub up; revives a runner that died while
//              want=run|pause (steps already done are skipped, see job.step); gives up after 5 restarts.
//
//    Job API (inside run.js):
//      job.dir, job.spec, job.state
//      await job.step(name, async () => value)   checkpointed: done steps are skipped on restart (returns the saved value)
//      await job.pool(n, items, async (item, i) => …)   at most n at once (default spec.concurrency, 2)
//      await job.claude({name, prompt, model, cwd, args, maxUsd, env, resume, baseCost, maxTurns})  → {text, cost, sid, ok, usage}
//              a `claude -p` session, tracked, killable, its cost added to state.spent; waits for free memory first
//              (min_mem_mb); refuses once the budget is spent. resume = fork that primed session (--fork-session).
//      job.total(n) · job.advance(k=1, msg) · job.note(markdown) · job.log(msg) · job.write(rel, text)
//      job.tell(text)   a message for the owner plane (--owner): an airport tile delivers it there (the end line goes too)
//      await job.gate()   wait while paused; throws if cancelled (pool/claude/step call it for you)
//      await job.sh(cmd, {cwd})  → {code, out}   a shell command in its own process group (killed on cancel)
//
//    CLI: ember.js new <id> [--title T] [--budget USD] [--concurrency N] [--from <template dir>] [--owner <plane>] [--card C]
//         ember.js start|pause|resume|stop <id> · ember.js ls · ember.js url [<id>] · ember.js hub · ember.js tick
//
// Settings: environment, else ~/.ember/.env (KEY=value lines; outside the repo, like secrets should be):
//   EMBER_ROOT · EMBER_PORT (63100) · EMBER_HOST (the name in job URLs; default this machine's hostname)
//   EMBER_KEY or EMBER_KEY_FILE (the hub's key; unset = open) · EMBER_TLS_CERT + EMBER_TLS_KEY (unset = plain http/1.1)
//   EMBER_CLAUDE_BIN (claude) · EMBER_CLAUDE_ENV_FILE (KEY=value lines added to every job.claude session, e.g. a token)
//   EMBER_END_HOOK: a command run when a job ends (env: EMBER_JOB_ID, EMBER_JOB_TITLE, EMBER_JOB_URL, EMBER_END_LINE,
//                   EMBER_JOB_CARD = the job's --card, EMBER_JOB_NOTIFY = 0|1), e.g. to push a phone notification
'use strict';
const fs = require('fs'), path = require('path'), os = require('os'), http = require('http'), https = require('https');
const crypto = require('crypto');
const { spawn, spawnSync } = require('child_process');

// settings: real env wins over ~/.ember/.env
const parseEnv = (f) => { const o = {}; try { for (const l of fs.readFileSync(f, 'utf8').split('\n')) { const m = l.match(/^\s*([A-Z0-9_]+)=(.*)$/); if (m) o[m[1]] = m[2].trim().replace(/^["']|["']$/g, ''); } } catch (_) {} return o; };
const CONF = parseEnv(path.join(os.homedir(), '.ember', '.env'));
const conf = (k, d) => process.env[k] || CONF[k] || d;
const ROOT = conf('EMBER_ROOT', __dirname);
const JOBS = path.join(ROOT, 'jobs');
const PORT = +conf('EMBER_PORT', 63100);
const HOST = conf('EMBER_HOST', os.hostname());
const NODE = process.execPath;
const MAX_RESTARTS = 5, BEAT_MS = 5000, STALE_MS = 30000;

// ---------- small helpers ----------
const now = () => new Date().toISOString();
const readJ = (f, d) => { try { return JSON.parse(fs.readFileSync(f, 'utf8')); } catch (_) { return d; } };
const writeJ = (f, o) => { const t = f + '.' + process.pid + '.tmp'; fs.writeFileSync(t, JSON.stringify(o, null, 1)); fs.renameSync(t, f); };
const CLAUDE = conf('EMBER_CLAUDE_BIN', 'claude');
const KEY = conf('EMBER_KEY') || (() => { try { return fs.readFileSync(conf('EMBER_KEY_FILE'), 'utf8').trim(); } catch (_) { return ''; } })();
const alive = (pid) => { if (!pid) return false; try { process.kill(pid, 0); return true; } catch (e) { return e.code === 'EPERM'; } };
const killGroup = (pid, sig = 'SIGTERM') => { if (!pid) return; try { process.kill(-pid, sig); } catch (_) { try { process.kill(pid, sig); } catch (_) {} } };
const memAvailMB = () => { try { return +fs.readFileSync('/proc/meminfo', 'utf8').match(/MemAvailable:\s+(\d+)/)[1] / 1024; } catch (_) { return 1e9; } };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const jdir = (id) => { if (!/^[a-z0-9][a-z0-9._-]{0,63}$/i.test(id || '')) throw new Error('bad job id: ' + id); return path.join(JOBS, id); };
const files = (id) => { const d = jdir(id); return { d, spec: path.join(d, 'job.json'), state: path.join(d, 'state.json'), control: path.join(d, 'control.json'), events: path.join(d, 'events.jsonl') }; };
const appendEvent = (id, ev) => fs.appendFileSync(files(id).events, JSON.stringify({ t: now(), ...ev }) + '\n');
const TERMINAL = new Set(['done', 'failed', 'cancelled']);
const TLS = !!(conf('EMBER_TLS_CERT') && conf('EMBER_TLS_KEY'));
const jobUrl = (id) => `${TLS ? 'https' : 'http'}://${HOST}:${PORT}/${id ? 'j/' + id + '/' : ''}${KEY ? '#k=' + KEY : ''}`;

// ---------- the runner (one per running job) ----------
class Cancelled extends Error { constructor() { super('cancelled'); this.cancelled = true; } }

async function runJob(id) {
  const F = files(id);
  const spec = readJ(F.spec, null); if (!spec) throw new Error('no job.json in ' + F.d);
  const prev = readJ(F.state, {});
  const st = Object.assign({ status: 'running', progress: { done: 0, total: 0, msg: '' }, steps: {}, sessions: {}, spent: 0, restarts: 0, notes: [] }, prev);
  st.status = 'running'; st.pid = process.pid; st.started = st.started || now(); st.run_started = now(); st.beat = now(); st.error = undefined;
  // sessions left "running" by a dead runner: kill their groups, mark lost (their step re-runs)
  for (const s of Object.values(st.sessions)) if (s.status === 'running') { killGroup(s.pid, 'SIGKILL'); s.status = 'lost'; s.ended = now(); }
  const save = () => { st.beat = now(); writeJ(F.state, st); };
  save();
  appendEvent(id, { kind: 'start', msg: prev.status ? `runner restarted (was ${prev.status})` : 'runner started', pid: process.pid });

  const live = new Map(); // pid → what
  let want = 'run';
  const readWant = () => { want = (readJ(F.control, {}).want) || 'run'; return want; };
  const cancelAll = () => { for (const pid of live.keys()) killGroup(pid); setTimeout(() => { for (const pid of live.keys()) killGroup(pid, 'SIGKILL'); }, 5000).unref(); };
  let acted = 'run';   // the want the heartbeat last acted on (gate() also calls readWant, so `want` alone can't see the change)
  const beat = setInterval(() => {
    const before = acted; acted = readWant();
    if (want === 'cancel' && before !== 'cancel') { appendEvent(id, { kind: 'cancel', msg: 'cancel asked: stopping ' + live.size + ' live process(es)' }); st.status = 'cancelling'; cancelAll(); }
    if (want === 'pause' && before !== 'pause') { appendEvent(id, { kind: 'pause', msg: 'paused: no new work starts; running work finishes' }); st.status = 'paused'; }
    if (want === 'run' && before === 'pause') { appendEvent(id, { kind: 'resume', msg: 'resumed' }); st.status = 'running'; }
    save();
  }, 1000);

  const job = {
    dir: F.d, spec, state: st, Cancelled,
    log(msg) { appendEvent(id, { kind: 'log', msg: String(msg) }); },
    note(md) { st.notes.push({ t: now(), md: String(md) }); save(); appendEvent(id, { kind: 'note', msg: String(md).split('\n')[0].slice(0, 200) }); },
    tell(text) { st.tell = [...(st.tell || []), { t: Date.now(), text: String(text).slice(0, 2000) }].slice(-3); save(); appendEvent(id, { kind: 'tell', msg: String(text).split('\n')[0].slice(0, 200) }); },
    total(n) { st.progress.total = n; save(); },
    advance(k = 1, msg = '') { st.progress.done += k; if (msg) st.progress.msg = msg; save(); appendEvent(id, { kind: 'progress', done: st.progress.done, total: st.progress.total, msg }); },
    write(rel, text) { const f = path.join(F.d, 'out', rel); if (!f.startsWith(path.join(F.d, 'out'))) throw new Error('bad out path'); fs.mkdirSync(path.dirname(f), { recursive: true }); fs.writeFileSync(f, text); return f; },
    async gate() {
      for (;;) { readWant(); if (want === 'cancel') throw new Cancelled(); if (want !== 'pause') return; await sleep(1000); }
    },
    async step(name, fn) {
      const s = st.steps[name];
      if (s && s.status === 'done') return s.value;
      await job.gate();
      st.steps[name] = { status: 'running', started: now() }; save();
      try {
        const value = await fn();
        st.steps[name] = { status: 'done', started: st.steps[name].started, ended: now(), value: value === undefined ? null : value }; save();
        return value;
      } catch (e) {
        st.steps[name] = { status: e.cancelled ? 'cancelled' : 'failed', started: st.steps[name].started, ended: now(), error: String(e.message || e).slice(0, 500) }; save();
        throw e;
      }
    },
    async pool(n, items, fn) {
      n = n || spec.concurrency || 2;
      let next = 0, firstErr = null; const errs = [];
      const worker = async () => {
        while (next < items.length && !firstErr) {
          const i = next++;
          try { await job.gate(); await fn(items[i], i); }
          catch (e) { if (e.cancelled) { firstErr = e; return; } errs.push({ i, e }); job.log(`item ${i} failed: ${e.message}`); }
        }
      };
      await Promise.all(Array.from({ length: Math.min(n, items.length) }, worker));
      if (firstErr) throw firstErr;
      return errs; // failed items (others still ran); the caller decides
    },
    async sh(cmd, o = {}) {
      await job.gate();
      return new Promise((res) => {
        const ch = spawn('bash', ['-lc', cmd], { cwd: o.cwd || F.d, detached: true, env: { ...process.env, ...(o.env || {}) } });
        live.set(ch.pid, 'sh'); let out = '';
        ch.stdout.on('data', d => out += d); ch.stderr.on('data', d => out += d);
        ch.on('close', (code) => { live.delete(ch.pid); res({ code, out }); });
      });
    },
    async claude(o) {
      await job.gate();
      const budget = spec.budget_usd;
      if (budget && st.spent >= budget) throw new Error(`budget spent ($${st.spent.toFixed(2)} of $${budget})`);
      const minMem = spec.min_mem_mb || 700;
      while (memAvailMB() < minMem) { st.progress.msg = `waiting for memory (${memAvailMB().toFixed(0)} MB free < ${minMem})`; save(); await sleep(5000); await job.gate(); }
      const name = (o.name || 's' + Object.keys(st.sessions).length).replace(/[^a-z0-9._-]/gi, '_');
      const sid = o.sid || crypto.randomUUID();
      const sdir = path.join(F.d, 'sessions'); fs.mkdirSync(sdir, { recursive: true });
      const logf = path.join(sdir, name + '.jsonl');
      const args = ['-p', '--output-format', 'stream-json', '--verbose', '--session-id', sid];
      if (o.resume) args.push('--resume', o.resume, '--fork-session');   // fork a primed session (its prompt cache is reused)
      if (o.model) args.push('--model', o.model);
      if (o.maxTurns) args.push('--max-turns', String(o.maxTurns));
      args.push(...(o.args || []));
      const env = { ...process.env, ...(o.env || {}) };
      if (conf('EMBER_CLAUDE_ENV_FILE')) Object.assign(env, parseEnv(conf('EMBER_CLAUDE_ENV_FILE')));
      const cwd = o.cwd || F.d;
      return new Promise((res, rej) => {
        let ch;
        try { ch = spawn(CLAUDE, args, { cwd, env, detached: true, stdio: ['pipe', 'pipe', 'pipe'] }); } catch (e) { return rej(e); }
        const rec = { name, sid, pid: ch.pid, status: 'running', started: now(), model: o.model || '', cwd, log: 'sessions/' + name + '.jsonl', cost: 0 };
        st.sessions[name] = rec; live.set(ch.pid, 'claude ' + name); save();
        appendEvent(id, { kind: 'session', msg: `▶ ${name} started`, name, sid });
        const out = fs.createWriteStream(logf, { flags: 'a' });
        let buf = '', result = null, errTxt = '';
        ch.stdout.on('data', (d) => {
          out.write(d); buf += d; let i;
          while ((i = buf.indexOf('\n')) >= 0) {
            const line = buf.slice(0, i); buf = buf.slice(i + 1);
            try { const ev = JSON.parse(line); if (ev.type === 'result') result = ev; } catch (_) {}
          }
        });
        ch.stderr.on('data', d => { errTxt += d; if (errTxt.length > 4000) errTxt = errTxt.slice(-4000); });
        ch.on('error', (e) => { errTxt += String(e); });
        ch.on('close', (code, sig) => {
          out.end(); live.delete(ch.pid);
          // total_cost_usd is cumulative over a resumed/forked session family: subtract what the parent had already cost
          const cost = Math.max(0, ((result && +result.total_cost_usd) || 0) - (+o.baseCost || 0));
          rec.cost = cost; st.spent = +(st.spent + cost).toFixed(4);
          rec.status = want === 'cancel' ? 'cancelled' : (code === 0 && result && !result.is_error ? 'done' : 'failed');
          rec.ended = now(); rec.code = code; if (rec.status === 'failed') rec.error = (errTxt || (result && result.result) || 'exit ' + code).slice(-500);
          save();
          appendEvent(id, { kind: 'session', msg: `■ ${name} ${rec.status} · $${cost.toFixed(3)}`, name, sid });
          if (want === 'cancel') return rej(new Cancelled());
          if (o.maxUsd && cost > o.maxUsd) job.log(`${name} cost $${cost.toFixed(2)} > its maxUsd $${o.maxUsd}`);
          res({ ok: rec.status === 'done', text: result ? String(result.result || '') : '', cost, sid, code, error: rec.error, usage: result && result.usage, total: result && +result.total_cost_usd });
        });
        ch.stdin.end(o.prompt || '');
      });
    },
  };

  let final = 'done', err = null;
  try {
    const runf = path.join(F.d, 'run.js');
    delete require.cache[require.resolve(runf)];
    await require(runf)(job);
  } catch (e) { err = e; final = e.cancelled ? 'cancelled' : 'failed'; }
  if (readWant() === 'cancel') final = 'cancelled';
  clearInterval(beat);
  cancelAll();
  st.status = final; st.ended = now(); st.pid = null; if (err && !err.cancelled) st.error = String(err.stack || err).slice(0, 2000);
  save();
  appendEvent(id, { kind: 'end', msg: `${final}${err && !err.cancelled ? ': ' + err.message : ''} · spent $${st.spent.toFixed(3)}` });
  finishHooks(id, spec, st);
}

const endLine = (id, st) => `ember ${id}: ${st.status} · ${(st.progress || {}).done || 0}/${(st.progress || {}).total || 0} · $${(st.spent || 0).toFixed(2)}`;
function finishHooks(id, spec, st) {
  // EMBER_END_HOOK (optional): your own notification / logging when a job ends. Best effort, 30 s max.
  const hook = conf('EMBER_END_HOOK');
  if (!hook) return;
  spawnSync(hook, [], { shell: true, timeout: 30000, stdio: 'ignore', env: { ...process.env, EMBER_JOB_ID: id, EMBER_JOB_TITLE: spec.title || id,
    EMBER_JOB_URL: jobUrl(id), EMBER_END_LINE: endLine(id, st), EMBER_JOB_CARD: spec.card || '', EMBER_JOB_NOTIFY: spec.notify === false ? '0' : '1' } });
}

// ---------- control (CLI and hub share these) ----------
function setWant(id, want) {
  const F = files(id);
  if (!fs.existsSync(F.spec)) throw new Error('no such job: ' + id);
  writeJ(F.control, { want, at: now() });
  appendEvent(id, { kind: 'control', msg: 'want → ' + want });
  if (want === 'run' || want === 'pause') ensureRunner(id, true);
  if (want === 'cancel') {
    // belt and braces: if the runner doesn't stop by itself in 15 s, kill its group and every session group
    const st = readJ(F.state, {});
    if (!alive(st.pid)) { markDead(id, 'cancelled'); return; }
    spawn(NODE, [__filename, '_reap', id], { detached: true, stdio: 'ignore' }).unref();
  }
}
function markDead(id, status, why) {
  const F = files(id); const st = readJ(F.state, {});
  for (const s of Object.values(st.sessions || {})) if (s.status === 'running') { killGroup(s.pid, 'SIGKILL'); s.status = status === 'cancelled' ? 'cancelled' : 'lost'; s.ended = now(); }
  st.status = status; st.pid = null; st.ended = now(); if (why) st.error = why;
  writeJ(F.state, st); appendEvent(id, { kind: 'end', msg: status + (why ? ': ' + why : '') });
}
async function reap(id) {
  await sleep(15000);
  const st = readJ(files(id).state, {});
  if (alive(st.pid)) { killGroup(st.pid, 'SIGKILL'); await sleep(500); markDead(id, 'cancelled', 'runner killed after 15 s'); }
}
function ensureRunner(id, fresh) {
  const F = files(id); const st = readJ(F.state, {});
  if (alive(st.pid)) return false;
  if (fresh) { st.restarts = 0; writeJ(F.state, st); }
  const logf = fs.openSync(path.join(F.d, 'runner.log'), 'a');
  const ch = spawn(NODE, [__filename, '_run', id], { detached: true, stdio: ['ignore', logf, logf], cwd: F.d });
  ch.unref();
  return true;
}
function listJobs() {
  return fs.readdirSync(JOBS).filter(n => fs.existsSync(path.join(JOBS, n, 'job.json'))).map(id => {
    const F = files(id); const spec = readJ(F.spec, {}); const st = readJ(F.state, {}); const c = readJ(F.control, {});
    let status = st.status || 'new';
    if (!TERMINAL.has(status) && status !== 'new' && !alive(st.pid)) status = 'stalled';
    return { id, title: spec.title || id, card: spec.card || '', status, want: c.want || '', progress: st.progress || {}, spent: st.spent || 0, budget: spec.budget_usd || null, created: spec.created, started: st.started, ended: st.ended, beat: st.beat, live: Object.values(st.sessions || {}).filter(s => s.status === 'running').length, sessions: Object.keys(st.sessions || {}).length };
  }).sort((a, b) => String(b.created).localeCompare(String(a.created)));
}

// ---------- tick (cron) ----------
function tick() {
  // 1. the hub
  const hubPid = +(readJ(path.join(ROOT, 'hub.json'), {}).pid || 0);
  if (!alive(hubPid)) {
    const logf = fs.openSync(path.join(ROOT, 'hub.log'), 'a');
    spawn(NODE, [__filename, 'hub'], { detached: true, stdio: ['ignore', logf, logf] }).unref();
    console.log(now(), 'hub started');
  }
  // 2. runners that died while we still want them
  for (const j of listJobs()) {
    if (TERMINAL.has(j.status) || j.status === 'new') continue;
    const F = files(j.id); const st = readJ(F.state, {}); const want = readJ(F.control, {}).want || 'run';
    if (alive(st.pid)) {
      if (st.beat && Date.now() - Date.parse(st.beat) > 5 * 60000) appendEvent(j.id, { kind: 'warn', msg: 'no heartbeat for > 5 min (runner hung?)' });
      continue;
    }
    if (want === 'cancel') { markDead(j.id, 'cancelled'); continue; }
    if ((st.restarts || 0) >= MAX_RESTARTS) { markDead(j.id, 'failed', `crash loop: ${MAX_RESTARTS} restarts`); continue; }
    st.restarts = (st.restarts || 0) + 1; writeJ(F.state, st);
    appendEvent(j.id, { kind: 'revive', msg: `runner was dead → restart ${st.restarts}/${MAX_RESTARTS}` });
    ensureRunner(j.id, false);
    console.log(now(), 'revived', j.id);
  }
}

// ---------- hub (https, one port for all jobs) ----------
function hub() {
  const CERT = conf('EMBER_TLS_CERT'), TKEY = conf('EMBER_TLS_KEY');
  const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.json': 'application/json', '.jsonl': 'text/plain; charset=utf-8', '.md': 'text/plain; charset=utf-8', '.txt': 'text/plain; charset=utf-8', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml', '.log': 'text/plain; charset=utf-8' };
  const send = (res, code, body, type = 'application/json') => { res.writeHead(code, { 'content-type': type, 'cache-control': 'no-store', 'referrer-policy': 'no-referrer', 'x-robots-tag': 'noindex' }); res.end(typeof body === 'string' || Buffer.isBuffer(body) ? body : JSON.stringify(body)); };
  const onReq = (req, res) => {
    const u = new URL(req.url, 'https://x');
    const p = decodeURIComponent(u.pathname);
    const isApi = p.startsWith('/api/');
    if (isApi && KEY && u.searchParams.get('k') !== KEY && req.headers['x-key'] !== KEY) return send(res, 401, { error: 'key' });
    try {
      if (p === '/' || p === '/index.html') return send(res, 200, fs.readFileSync(path.join(__dirname, 'hub.html')), MIME['.html']);
      if (p === '/api/jobs') return send(res, 200, listJobs());
      let m;
      if ((m = p.match(/^\/api\/j\/([^/]+)\/state$/))) {
        const F = files(m[1]); return send(res, 200, { spec: readJ(F.spec, {}), state: readJ(F.state, {}), control: readJ(F.control, {}), list: listJobs().find(j => j.id === m[1]) || null, outs: listOut(F.d) });
      }
      if ((m = p.match(/^\/api\/j\/([^/]+)\/(events|session)$/))) {
        const F = files(m[1]);
        let f = F.events;
        if (m[2] === 'session') { const nm = (u.searchParams.get('name') || '').replace(/[^a-z0-9._-]/gi, '_'); f = path.join(F.d, 'sessions', nm + '.jsonl'); }
        const from = +(u.searchParams.get('from') || 0);
        let size = 0; try { size = fs.statSync(f).size; } catch (_) { return send(res, 200, { next: 0, text: '' }); }
        const start = from > size ? 0 : from, len = Math.min(size - start, 512 * 1024);
        const b = Buffer.alloc(len); const fd = fs.openSync(f, 'r'); fs.readSync(fd, b, 0, len, start); fs.closeSync(fd);
        let text = b.toString('utf8'); const cut = text.lastIndexOf('\n') + 1; text = text.slice(0, cut);
        return send(res, 200, { next: start + Buffer.byteLength(text), size, text });
      }
      if ((m = p.match(/^\/api\/j\/([^/]+)\/control$/)) && req.method === 'POST') {
        let body = ''; req.on('data', d => body += d); req.on('end', () => {
          try { const want = (JSON.parse(body || '{}').want || ''); if (!['run', 'pause', 'cancel'].includes(want)) throw new Error('want must be run|pause|cancel'); setWant(m[1], want); send(res, 200, { ok: true, want }); }
          catch (e) { send(res, 400, { error: e.message }); }
        }); return;
      }
      // the ember contract (github.com/dglittle/ember): every job is an ember with base URL /j/<id>/
      if ((m = p.match(/^\/j\/([^/]+)\/(state|control)$/))) {
        const auth = req.headers.authorization || '';
        if (KEY && u.searchParams.get('k') !== KEY && req.headers['x-key'] !== KEY && auth !== 'Bearer ' + KEY) return send(res, 401, { ok: false, error: 'key' });
        if (!fs.existsSync(files(m[1]).spec)) return send(res, 404, { ok: false, error: 'no such job' });
        if (m[2] === 'state' && req.method === 'GET') return subscribe(m[1], req, res);
        if (m[2] === 'control' && req.method === 'POST') {
          let body = ''; req.on('data', d => { body += d; if (body.length > 1e5) req.destroy(); }); req.on('end', () => {
            let action = ''; try { action = String(JSON.parse(body || '{}').action || ''); } catch (_) {}
            const cur = contractState(m[1]);
            // close: the hub drops nothing (it serves every job), but a running job is stopped: nobody needs it any more
            if (action === 'close') { if (!C_FINAL.has(cur.status)) { try { setWant(m[1], 'cancel'); } catch (_) {} } return send(res, 200, { ok: true, status: C_FINAL.has(cur.status) ? cur.status : 'stopping' }); }
            const want = { stop: 'cancel', pause: 'pause', resume: 'run' }[action];
            if (!want) return send(res, 409, { ok: false, error: 'action must be ' + Object.keys(cur.controls || {}).join('|') });
            if (C_FINAL.has(cur.status)) return send(res, 409, { ok: false, error: 'already ' + cur.status });
            try { setWant(m[1], want); } catch (e) { return send(res, 409, { ok: false, error: e.message }); }
            const after = { cancel: 'stopping', pause: 'paused', run: 'running' }[want];
            send(res, 200, { ok: true, status: after });
          }); return;
        }
        return send(res, 405, { ok: false, error: 'GET state · POST control' });
      }
      if ((m = p.match(/^\/j\/([^/]+)\/?(.*)$/))) {
        const F = files(m[1]); let rel = m[2] || 'index.html';
        if (rel === 'ember') return send(res, 200, fs.readFileSync(path.join(__dirname, 'page.html')), MIME['.html']);   // the generic ember view, even when the job has its own index.html
        if (rel.endsWith('/')) rel += 'index.html';
        // pages are open (they carry no data; their API calls need the key); everything else needs the key
        const f = path.resolve(F.d, rel);
        if (!f.startsWith(F.d + path.sep)) return send(res, 403, 'no');
        if (rel !== 'index.html' && KEY && u.searchParams.get('k') !== KEY) return send(res, 401, 'key (add ?k=…)', 'text/plain');
        if (/(^|\/)(\.|.*\.env$)/.test(rel)) return send(res, 403, 'no');
        if (!fs.existsSync(f)) {
          if (rel === 'index.html') return send(res, 200, fs.readFileSync(path.join(__dirname, 'page.html')), MIME['.html']);
          return send(res, 404, 'not found', 'text/plain');
        }
        if (fs.statSync(f).isDirectory()) return send(res, 200, fs.readdirSync(f).join('\n'), 'text/plain');
        return send(res, 200, fs.readFileSync(f), MIME[path.extname(f)] || 'application/octet-stream');
      }
      return send(res, 404, { error: 'not found' });
    } catch (e) { return send(res, 500, { error: e.message }); }
  };
  // HTTP/2 (the contract's transport: one connection carries many /state streams); HTTP/1.1 still accepted
  const server = CERT && TKEY ? require('http2').createSecureServer({ cert: fs.readFileSync(CERT), key: fs.readFileSync(TKEY), allowHTTP1: true }, onReq) : http.createServer(onReq);
  setInterval(pollEmbers, 1000);
  setInterval(() => { for (const e of embers.values()) for (const r of e.subs) r.write(': ping\n\n'); }, 15000);
  server.listen(PORT, () => { writeJ(path.join(ROOT, 'hub.json'), { pid: process.pid, port: PORT, at: now() }); console.log(now(), `🔥 ember hub on ${CERT ? 'https' : 'http'}://${HOST}:${PORT}/`); });
}
// ---------- the ember contract, v1 (github.com/dglittle/ember) ----------
// Each job is an ember at /j/<id>/: GET state = SSE (event "shelf" first, then "delta"s), POST control
// {action: stop|pause|resume}. The runner doesn't know: the hub maps the job's files onto the contract's
// state keys once a second for jobs someone is watching, and sends only what changed (vanished keys → null).
const shelf_merge = require('./shelf.js');
const C_STATUS = { new: 'starting', running: 'running', paused: 'paused', cancelling: 'stopping', done: 'done', failed: 'failed', cancelled: 'stopped' };
const C_FINAL = new Set(['done', 'failed', 'stopped']);
const msOf = (iso) => (iso ? Date.parse(iso) || null : null);
function contractState(id) {
  const F = files(id); const spec = readJ(F.spec, {}); const st = readJ(F.state, {});
  const status = C_STATUS[st.status || 'new'] || String(st.status);
  const final = C_FINAL.has(status), p = st.progress || {};
  const err = st.error ? String(st.error).split('\n')[0].slice(0, 300) : null;
  // tell = the job's own job.tell() lines + the end line, newest last, last 3
  const tell = (Array.isArray(st.tell) ? st.tell : []).filter((x) => x && x.t).map((x) => ({ t: +x.t, text: String(x.text || '') }));
  if (final && st.ended) tell.push({ t: msOf(st.ended), text: endLine(id, st) + (err ? ' · ' + err : '') });
  tell.sort((a, b) => a.t - b.t);
  return {
    ember: 1, title: spec.title || id, emoji: '🔥', status,
    progress: { done: +p.done || 0, total: +p.total || 0, msg: String(p.msg || '') },
    started: msOf(st.started), ended: final ? msOf(st.ended) : null, beat: msOf(st.beat),
    error: err, spent: { usd: +(st.spent || 0), budget: spec.budget_usd || null },
    controls: final ? { close: true } : { stop: true, pause: true, resume: true, close: true },
    owner: spec.owner && spec.owner.plane ? { tower: spec.owner.tower || '', plane: spec.owner.plane } : null,
    tell: tell.slice(-3),
    live: Object.values(st.sessions || {}).filter((s) => s.status === 'running').length,   // the job's own: running claude sessions
  };
}
// a shelf patch that turns cur into next: changed keys, plus null for every key that vanished
function diffPatch(cur, next) {
  const isObj = (o) => o && typeof o === 'object' && !Array.isArray(o);
  const p = {}; let any = false;
  for (const k of Object.keys(next)) {
    const a = cur[k], b = next[k];
    if (isObj(a) && isObj(b)) { const d = diffPatch(a, b); if (d) { p[k] = d; any = true; } }
    else if (b == null) { if (a != null) { p[k] = null; any = true; } }
    else if (JSON.stringify(a) !== JSON.stringify(b)) { p[k] = b; any = true; }
  }
  for (const k of Object.keys(cur)) if (!(k in next)) { p[k] = null; any = true; }
  return any ? p : null;
}
const embers = new Map();   // job id → {shelf, subs}; only while someone watches
function refreshEmber(id, e) {
  const next = contractState(id), cur = e.shelf[0] || {};
  // beat: the runner writes it every second; the contract needs ≤ 30 s. Pass it on every ~10 s.
  if (cur.beat && next.beat && next.beat > cur.beat && next.beat - cur.beat < 10000) next.beat = cur.beat;
  const patch = diffPatch(cur, next);
  return patch ? shelf_merge(e.shelf, [patch, null]) : null;
}
function subscribe(id, req, res) {
  let e = embers.get(id);
  if (!e) { e = { shelf: [null, null], subs: new Set() }; refreshEmber(id, e); embers.set(id, e); }
  res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store', 'x-accel-buffering': 'no' });
  res.write(`event: shelf\ndata: ${JSON.stringify(e.shelf)}\n\n`);
  e.subs.add(res);
  res.on('close', () => { e.subs.delete(res); if (!e.subs.size && embers.get(id) === e) embers.delete(id); });
}
function pollEmbers() {
  for (const [id, e] of embers) {
    let delta = null; try { delta = refreshEmber(id, e); } catch (_) {}
    if (delta) for (const r of e.subs) r.write(`event: delta\ndata: ${JSON.stringify(delta)}\n\n`);
  }
}

function listOut(d) {
  const o = path.join(d, 'out'); const r = [];
  const walk = (x, rel) => { for (const n of fs.readdirSync(x)) { const f = path.join(x, n); const s = fs.statSync(f); if (s.isDirectory()) { if (r.length < 500) walk(f, rel + n + '/'); } else r.push({ path: 'out/' + rel + n, size: s.size, mtime: s.mtime }); } };
  try { walk(o, ''); } catch (_) {}
  return r.slice(0, 500);
}

// ---------- new ----------
function newJob(id, o) {
  const d = jdir(id);
  if (fs.existsSync(path.join(d, 'job.json'))) throw new Error('job exists: ' + id);
  fs.mkdirSync(path.join(d, 'out'), { recursive: true });
  const spec = { id, title: o.title || id, card: o.card || '', budget_usd: o.budget ? +o.budget : null, concurrency: o.concurrency ? +o.concurrency : 2, min_mem_mb: 700, notify: true, created: now() };
  // --owner <plane>: the airport plane this job reports to (its tile delivers job.tell + the end line there)
  if (o.owner && o.owner !== true) spec.owner = { tower: (o.tower && o.tower !== true ? o.tower : process.env.AIRPORT_URL) || '', plane: String(o.owner) };
  writeJ(path.join(d, 'job.json'), spec);
  const tpl = o.from || path.join(__dirname, 'templates', 'blank');
  for (const n of fs.readdirSync(tpl)) fs.copyFileSync(path.join(tpl, n), path.join(d, n));
  if (!fs.existsSync(path.join(d, 'run.js'))) fs.writeFileSync(path.join(d, 'run.js'), "module.exports = async (job) => {\n  job.note('nothing to do yet: write run.js');\n};\n");
  appendEvent(id, { kind: 'new', msg: 'created' });
  return d;
}

// ---------- the client: any ember, by URL ----------
function client(cmd, raw) {
  const http2 = require('http2');
  const shelf_merge = require('./shelf.js');
  const insecure = process.argv.includes('--insecure') || process.env.EMBER_INSECURE === '1';

  const u = new URL(raw);
  const key = new URLSearchParams(u.hash.slice(1)).get('k') || u.searchParams.get('k') || process.env.EMBER_KEY || '';
  const base = (u.pathname.endsWith('/') ? u.pathname : u.pathname + '/');
  const headers = (extra) => Object.assign(key ? { authorization: 'Bearer ' + key } : {}, extra);
  const die = (msg) => { console.error('ember: ' + msg); process.exit(1); };

  const client = http2.connect(u.origin, { rejectUnauthorized: !insecure });
  client.on('error', (e) => die(e.message));

  if (['stop', 'pause', 'resume', 'close'].includes(cmd)) {
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
}

// ---------- CLI ----------
async function main() {
  const argv = process.argv.slice(2).filter((a) => a !== '--insecure');
  if (/^https?:\/\//.test(argv[1] || '')) return client(argv[0], argv[1]);   // a URL: the client, for any ember
  const [cmd, id, ...rest] = argv;
  if (cmd) fs.mkdirSync(JOBS, { recursive: true });
  const opt = {}; for (let i = 0; i < rest.length; i++) if (rest[i].startsWith('--')) opt[rest[i].slice(2)] = rest[i + 1] && !rest[i + 1].startsWith('--') ? rest[++i] : true;
  switch (cmd) {
    case 'new': console.log(newJob(id, opt)); console.log(jobUrl(id)); break;
    case 'start': case 'resume': setWant(id, 'run'); console.log(jobUrl(id)); break;
    case 'pause': setWant(id, 'pause'); break;
    case 'cancel': case 'stop': setWant(id, 'cancel'); break;
    case 'ls': for (const j of listJobs()) console.log(`${j.status.padEnd(10)} ${j.id.padEnd(24)} ${String(j.progress.done || 0)}/${j.progress.total || 0}  $${j.spent.toFixed(2)}  live ${j.live}  ${j.title}`); break;
    case 'url': console.log(jobUrl(id)); break;
    case 'hub': hub(); break;
    case 'tick': tick(); break;
    case '_run': await runJob(id); process.exit(0); break;
    case '_reap': await reap(id); break;
    default: { const h = []; for (const l of fs.readFileSync(__filename, 'utf8').split('\n').slice(1)) { if (!l.startsWith('//')) break; h.push(l.slice(3)); } console.log(h.join('\n')); }
  }
}
if (require.main === module) main().catch(e => { console.error('ember:', e.message); process.exit(1); });
