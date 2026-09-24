# Async Subagent Callback

## Problem

How can an orchestrator launch a long-running subagent task without blocking,
while still reliably learning the result asynchronously even when the
subagent crashes, hangs or the callback itself fails? The full reasoning and design tradeoffs for each stage are in [`my blog`](https://chuhong-wang.github.io/blog/subagent-callback).

## Architecture

```
Orchestrator ──POST /execute──►  Subagent      (returns immediately)
Orchestrator ◄──POST /callback── Subagent      (when work completes)
Orchestrator ◄──POST /heartbeat─ Subagent      (periodically, while working)
```

## Design evolution

The code is built up one failure mode at a time. Each numbered directory is a
runnable `orchestrator.py` / `subagent.py` pair.

| Stage | Directory | Adds |
|---|---|---|
| 1 | `01_basic_callback/` | execute + callback ("nothing goes wrong" happy path) |
| 2 | `02_failure_callback/` | try/except → explicit failure callback |
| 3 | `03_timeout/` | orchestrator-side watchdog for silent subagents |
| 4 | `04_heartbeat/` | heartbeats for fast subagent stall detection |
| 5 | `05_retry_idempotency/` | callback retries + idempotent handler |

## Failure modes covered

1. Task logic raises → failure callback (stage 2)
2. Subagent crashes / hangs, sends nothing → watchdog timeout (stage 3)
3. Subagent stalls mid-task → missed heartbeats (stage 4)
4. Callback request fails → retry with backoff (stage 5)
5. Duplicate callback delivery → idempotent handler (stage 5)

## How to Run locally

Create the environment once:

```bash
micromamba create -y -n subagent-cb python=3.12 -c conda-forge
micromamba run -n subagent-cb pip install fastapi "uvicorn[standard]" httpx pydantic
```

Then, from inside a stage directory (e.g. `03_timeout/`), run the two processes
in separate terminals:

```bash
micromamba activate subagent-cb
python orchestrator.py     # http://localhost:8000
python subagent.py   # http://localhost:8001
```

Trigger a task and poll where it ended up:

```bash
curl "http://localhost:8000/start"
curl "http://localhost:8000/status/<task_id>"
```

> Launching both processes with `micromamba run` *simultaneously* can trip a
> transient mamba lock. Activate the env first and run plain `python`, or stagger
> the two launches by a second.

### Exercising each failure path

The `/start` endpoint takes flags that drive the subagent into each failure mode.
Flags are only wired up in the stage that introduces the relevant handling.

| Command | Stage | What it does |
|---|---|---|
| `GET /start` | 1+ | happy path → `completed` |
| `GET /start?fail=true` | 2+ | task raises → `failed` callback |
| `GET /start?hang=true` | 3, 4 | subagent never calls back → orchestrator watchdog → `timed_out` |
| `GET /start?stall=true` | 4 | beats a few times then goes silent → `failed` via missed heartbeats |
| `GET /status/{task_id}` | 1+ | current/terminal state of a task |

Stage 5 has no flags: its orchestrator deliberately rejects the first two callback
attempts to force the retry path, and `/status` reports `callback_attempts_seen`
so you can watch retries and duplicates arrive while the recorded state stays
stable. To see idempotency directly, let a task complete, then manually POST a
different terminal callback for the same `task_id` — it is acknowledged as a
duplicate and does not overwrite the recorded state.
