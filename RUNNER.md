# The ember job runner (optional)

`ember.js` also runs **long-running jobs as directories**, and serves every job as an ember through one hub. You
don't need it to make an ember (any server that speaks [the contract](README.md) is one), but it handles the
common case: a script that runs for hours, maybe fanning out to many `claude -p` sessions, and has to survive
crashes and reboots.

## Use

    E="node ember.js"
    $E new r1 --title "20 more samples" --budget 80 --concurrency 2 --owner <plane>
    # write jobs/r1/run.js   (see templates/demo/run.js and the job API in ember.js's header)
    $E start r1                      # prints the job's URL (an ember: /j/r1/ · /j/r1/state · /j/r1/control)
    $E ls · $E pause|resume|stop r1 · $E url r1

Keep it alive with cron: `* * * * * node /path/to/ember.js tick`. It restarts the hub if it's down and revives any
runner that died while its job should be running.

## How it works

- **The directory is the truth.** It holds:
  - `job.json` (the spec)
  - `run.js` (the logic)
  - `control.json` (what we want, written by the buttons and CLI)
  - `state.json` (what is, written only by the runner)
  - `events.jsonl`
  - `sessions/<name>.jsonl` (the raw stream of each claude session)
  - `out/`
- **The runner** is `ember.js _run <id>`, detached in its own process group. Every claude or shell child is in its
  own group too, so stop kills whole trees. It sends a heartbeat every second and reads `control.json` every second.
- **`job.step(name, fn)` is checkpointed.** After a crash or revive, finished steps are skipped, so write steps to
  be safe to re-run.
- **Guards:**
  - `budget_usd` (no new sessions once it's spent)
  - `concurrency` (default 2)
  - `min_mem_mb` (waits for that much free memory before starting a session; default 700)
- **The hub** maps each job's files onto the contract's state (status, progress, spent, controls, owner, tell), so
  the runners don't need to know about it. It serves HTTP/2.
- **Messages to the owner.** `job.tell(text)`, plus an automatic end line (`ember r1: done · 6/6 · $1.23`), go into
  the ember's `tell`. Airport delivers them to the `--owner` plane as a new turn.

## Settings

Environment variables, else `~/.ember/.env`. Keep it outside the repo, because it can point at keys.
- `EMBER_ROOT`: where `jobs/` lives (default: next to ember.js)
- `EMBER_PORT` (63100) and `EMBER_HOST` (the name used in job URLs)
- `EMBER_KEY` or `EMBER_KEY_FILE`: the hub's key
- `EMBER_TLS_CERT` + `EMBER_TLS_KEY`
- `EMBER_CLAUDE_BIN`, and `EMBER_CLAUDE_ENV_FILE` (extra env for every job.claude session, e.g. an OAuth token)
- `EMBER_END_HOOK`: a command run when a job ends. It receives `EMBER_JOB_ID`, `EMBER_JOB_TITLE`, `EMBER_JOB_URL`,
  `EMBER_END_LINE`, `EMBER_JOB_CARD` (the job's `--card`, any tag you like) and `EMBER_JOB_NOTIFY`. Use it for phone
  pushes, logs, and the like.
