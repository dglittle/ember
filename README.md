# 🔥 ember

**An ember is long-running work that has a URL.** Open the URL in a browser and you get a live dashboard with a stop button. Point a program at the same URL and it can watch the work and steer it.

ember is a contract, not a framework. *How* the work runs is up to the ember: a script that loops, a job that fans out to many Claude sessions, something that waits on CI. Anything that serves the three endpoints below is an ember. This repo holds the contract, a minimal example to copy from, and a command-line client.

It was made for [airport](https://github.com/dglittle/airport), where Claude sessions ("planes") start embers to do work too long, too wide or too mechanical for a conversation. An agent's turn ends when it replies, but an ember keeps going. Airport shows embers as tiles, and when an ember finishes (or has something to say) the message goes back to the plane that owns it.

## The contract (v1)

### Transport

- **HTTP/2 over TLS** (ALPN `h2`). An ember may also accept HTTP/1.1. With HTTP/2, one connection carries any number of SSE streams, so a dashboard or a tower can watch many embers over one socket.
- **Auth:** an optional key, sent as `Authorization: Bearer <key>` or `?k=<key>`. The `?k=` form exists because `EventSource` can't set headers. The dashboard page itself needs no key, since it carries no data; it reads the key from its `#k=` fragment and adds it to its own requests.

### Endpoints

Paths are relative to the ember's base URL `B/`. Embers can live under a path, like `https://host/e/name/`, so dashboards use relative URLs.

| | |
|---|---|
| `GET B/` | **the dashboard** (HTML). Show whatever suits the work. |
| `GET B/state` | **the state, as Server-Sent Events** (see below). This is the only way to read state. |
| `POST B/control` `{"action":"stop"}` | **stop the work** → `{"ok":true,"status":…}`, or `{"ok":false,"error":…}` (409). `"pause"` and `"resume"` too, if `controls` lists them. |

**After the work ends, an ember keeps serving** all three endpoints (the final state, and a dashboard without live controls) until someone removes it. The page is often most useful once the work is done.

### `B/state`: the SSE stream

```
event: shelf
data: [{"ember":1,"title":"…","status":"running",…},{…versions…}]

event: delta
data: [{"progress":{"done":2,"msg":"step 2 of 8"}},[1,{"progress":[1,{"done":3,"msg":3}]}]]

: ping
```

- **The first event is always `shelf`:** the whole state as a [shelf](https://github.com/dglittle/shelf), `[value, versions]`.
- **Every change after that is a `delta`** event, which is also a shelf.
- **Clients apply both with `shelf_merge`** ([`shelf.js`](shelf.js)): start from `[null, null]`, merge the `shelf` event, then merge each `delta` as it arrives.
- **A `: ping` comment arrives at least every 15 s.**
- **On reconnect** the client gets a fresh `shelf` event, so there's nothing to catch up on.

### The state

The ember is the **only writer**. Clients only read the state; commands go through `POST /control`.

| key | type | meaning |
|---|---|---|
| `ember` | `1` | contract version |
| `title` | string | |
| `emoji` | string | optional |
| `status` | `"starting"` `"running"` `"paused"` `"stopping"` `"done"` `"failed"` `"stopped"` | the last three are final |
| `progress` | `{done, total, msg}` | `total` may be 0 (unknown) |
| `started`, `ended` | ms since the Unix epoch | `ended` is absent until the work ends |
| `beat` | ms since the Unix epoch | updated at least every 30 s until the work ends. A client treats `running` or `paused` with `now − beat > 90 s` as **stalled** (the work died but the server didn't) |
| `error` | string | when `failed` (or anything worth showing) |
| `spent` | `{usd, budget}` | optional |
| `controls` | `{stop: true, pause?: true, resume?: true}` | the actions `/control` accepts |
| `owner` | `{tower, plane}` | optional: the airport plane this ember reports to |
| `tell` | `[{t, text}, …]` | **messages for the owner**: the last 3, newest last. `t` (ms) identifies a message; a reader delivers each `t` it hasn't seen yet |

Any other key is the ember's own, for its dashboard.

**Shelf rules** (they keep every client in sync):
1. **Delete a key by setting it to `null`.** A key that silently disappears stays alive in every client.
2. **Arrays are replaced whole.** Keep them small, like `tell`. Put collections in objects keyed by id, and put history somewhere else (a file, the dashboard).
3. **Keep the state small** (≈ 64 KB or less). It's resent whole to every new subscriber.
4. **Don't flood.** At most a few changes per second; batch bursts.

## Try it

You need Node (built-ins only; tested on Node 24) and `openssl` for a localhost certificate:

```bash
git clone https://github.com/dglittle/ember && cd ember
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 365 \
  -subj /CN=localhost -keyout key.pem -out cert.pem
EMBER_KEY=sekrit node example.js            # → 🔥 example ember on https://localhost:63200/#k=sekrit
```

Open that link (your browser will warn about the self-signed certificate). In another terminal:

```bash
node ember.js get   'https://localhost:63200/#k=sekrit' --insecure   # the state, as JSON
node ember.js watch 'https://localhost:63200/#k=sekrit' --insecure   # a line per change, until it ends
node ember.js pause 'https://localhost:63200/#k=sekrit' --insecure   # also: resume, stop
curl -skN --http2 -H 'Authorization: Bearer sekrit' https://localhost:63200/state   # the raw stream
```

`--insecure` is only for the self-signed certificate. With a real one, drop it.

## Building your own ember

Copy [`example.js`](example.js), [`example.html`](example.html) and [`shelf.js`](shelf.js), then replace `tick()` with the real work. The checklist:

1. **Serve the three endpoints over HTTP/2 + TLS.** Node: `http2.createSecureServer({cert, key, allowHTTP1: true}, handler)`.
2. **Hold the state in a shelf, and write it only through one function:**
   ```js
   function set(patch) {
     const delta = shelf_merge(shelf, [patch, null]);           // [patch, null] = a local write
     if (delta) for (const res of subscribers) res.write(`event: delta\ndata: ${JSON.stringify(delta)}\n\n`);
   }
   ```
3. **For `GET /state`:** respond `200` with `content-type: text/event-stream`, write `event: shelf` with the whole shelf, add the response to the subscribers, and remove it on `close`. Ping every 15 s.
4. **Keep `beat` fresh** while the work runs. Set `status`, `ended` and maybe `error` when it stops.
5. **Make `stop` actually stop the work,** including child processes (start them in their own process group and kill the group).
6. **When the work ends** (or needs a human), push a line onto `tell`, keeping the last 3.
7. **Keep serving after the work ends.**

Some suggestions for long work:
- **Make it survive restarts:** save progress to disk, and on start skip what's done.
- **Guard the machine:** cap concurrency and wait for free memory before starting heavy children.
- **Put a budget in `spent`** and refuse new paid work once it's spent.

None of these is part of the contract, but each has saved real runs.

## Files

| | |
|---|---|
| `shelf.js` | `shelf_merge`, verbatim from [dglittle/shelf](https://github.com/dglittle/shelf); works in Node and in the browser |
| `example.js` + `example.html` | a minimal ember: counts to N, with pause/resume/stop |
| `ember.js` | the client: `get`, `watch`, `stop`, `pause`, `resume` |

## Not in v1 (added when something uses it)

- steering input (`POST /input`)
- result files (`GET /out/…`)
- a log stream
- a list of the Claude sessions an ember ran, so airport can open one as a plane
- one front door serving many embers under `/e/<name>/`

## License

MIT, see [LICENSE](LICENSE).
