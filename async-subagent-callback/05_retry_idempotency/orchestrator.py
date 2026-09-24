import asyncio
import time
import uuid

import httpx
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

SUBAGENT_URL = "http://localhost:8001"

TASK_TIMEOUT_SECONDS = 60.0
HEARTBEAT_INTERVAL_SECONDS = 2.0
HEARTBEAT_DEADLINE_SECONDS = HEARTBEAT_INTERVAL_SECONDS * 2.5

# Simulate a flaky orchestrator: reject the first N callback attempts per task so the
# subagent is forced to retry, and duplicate deliveries can be exercised.
FLAKY_CALLBACK_REJECTS = 2

tasks: dict[str, dict] = {}
# Count how many callback POSTs we've *seen* per task (including duplicates and
# rejected attempts), to prove idempotency: the outcome is written once no
# matter how many times the callback lands.
callback_attempts: dict[str, int] = {}


class CallbackPayload(BaseModel):
    task_id: str
    status: str
    result: str | None = None
    error: str | None = None


class HeartbeatPayload(BaseModel):
    task_id: str


async def liveness_monitor(task_id: str):
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
        task = tasks.get(task_id)
        if task is None or task["status"] != "running":
            return
        now = time.monotonic()
        if now - task["last_heartbeat"] > HEARTBEAT_DEADLINE_SECONDS:
            task.update(status="failed", error="missed heartbeats")
            return
        if now - task["started_at"] > TASK_TIMEOUT_SECONDS:
            task.update(status="timed_out", error="exceeded absolute deadline")
            return


@app.get("/start")
async def start_task(fail: bool = False):
    task_id = str(uuid.uuid4())
    now = time.monotonic()
    tasks[task_id] = {
        "status": "running", "result": None, "error": None,
        "started_at": now, "last_heartbeat": now,
    }
    callback_attempts[task_id] = 0

    async with httpx.AsyncClient() as client:
        await client.post(
            f"{SUBAGENT_URL}/execute",
            json={
                "task_id": task_id,
                "callback_url": "http://localhost:8000/callback",
                "heartbeat_url": "http://localhost:8000/heartbeat",
                "heartbeat_interval": HEARTBEAT_INTERVAL_SECONDS,
                "should_fail": fail,
            },
        )
    asyncio.create_task(liveness_monitor(task_id))
    return {"task_id": task_id, "status": "subagent_started"}


@app.post("/heartbeat")
async def receive_heartbeat(payload: HeartbeatPayload):
    task = tasks.get(payload.task_id)
    if task and task["status"] == "running":
        task["last_heartbeat"] = time.monotonic()
    return {"received": True}


@app.post("/callback")
async def receive_callback(payload: CallbackPayload):
    """Idempotent terminal-state writer, exposed to an unreliable network.

    Two independent things happen here:

    1. Simulated flakiness: the first FLAKY_CALLBACK_REJECTS attempts return
       503 so the subagent must retry. This models a orchestrator that is briefly
       down or overloaded.

    2. Idempotency: once a terminal state is recorded, any further callback for
       that task (a retry that actually did get through the first time, or a
       genuine duplicate delivery) is acknowledged with 200 but does NOT change
       state. This is what makes retries safe -- "at least once" delivery plus
       an idempotent handler gives "effectively once" semantics.

    The 200-on-duplicate is deliberate: returning an error would make the
    subagent retry forever. A duplicate is success from the sender's view.
    """
    task_id = payload.task_id
    callback_attempts[task_id] = callback_attempts.get(task_id, 0) + 1
    attempt = callback_attempts[task_id]

    if attempt <= FLAKY_CALLBACK_REJECTS:
        print(f"[Orchestrator] Rejecting callback attempt #{attempt} for {task_id} (flaky)")
        # 503 -> subagent should treat as retryable.
        return _http_error(503, "temporarily unavailable")

    task = tasks.get(task_id)
    if task is None:
        tasks[task_id] = {"status": payload.status}
        return {"received": True, "task_id": task_id}

    if task["status"] != "running":
        # Already terminal: duplicate / late delivery. Ack without mutating.
        print(f"[Orchestrator] Duplicate callback for {task_id} "
              f"(attempt #{attempt}); already {task['status']}, ignoring")
        return {"received": True, "task_id": task_id, "duplicate": True}

    print(f"[Orchestrator] Recording terminal state for {task_id}: {payload.status} "
          f"(attempt #{attempt})")
    task.update(status=payload.status, result=payload.result, error=payload.error)
    return {"received": True, "task_id": task_id}


def _http_error(code: int, detail: str):
    from fastapi import HTTPException
    raise HTTPException(status_code=code, detail=detail)


@app.get("/status/{task_id}")
async def get_status(task_id: str):
    return {
        **tasks.get(task_id, {"status": "unknown"}),
        "callback_attempts_seen": callback_attempts.get(task_id, 0),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
