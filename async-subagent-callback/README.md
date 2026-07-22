# Async Subagent Callback

## Problem

How can an orchestrator launch a long-running subagent task
without blocking, while still receiving the result asynchronously?

## Architecture

```
Parent ──execute──► Subagent
Parent ◄──callback── Subagent
```

## Minimal Implementation

```
@app.post("/callback")
async def callback(payload):
    update_task_state(payload)
```

## Failure Modes

1. Subagent explicitly fails
2. Subagent crashes
3. Subagent hangs
4. Callback request fails

## Design Evolution

Callback
    ↓
Failure callback
    ↓
Timeout
    ↓
Heartbeat
    ↓
Callback Retry + idempotency

## Running Locally

[commands]

## Production Considerations

- Durable task state
- Authentication
- Idempotency
- Retry policy
- Heartbeats / leases
- Observability

## Limitations

This implementation uses in-memory state and is intended
as a minimal reference implementation.