import asyncio
import time
import uuid

import httpx
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

SUBAGENT_URL = "http://localhost:8001"

# A coarse absolute deadline still exists as a backstop for tasks that make
# steady progress but simply run too long.
TASK_TIMEOUT_SECONDS = 60.0

# Heartbeat tuning. The subagent is expected to ping every HEARTBEAT_INTERVAL.
# We declare it dead if we miss roughly LIVENESS_MULTIPLIER intervals -- one
# missed beat can be a hiccup, several in a row means it stopped.
HEARTBEAT_INTERVAL_SECONDS = 2.0
LIVENESS_MULTIPLIER = 2.5
HEARTBEAT_DEADLINE_SECONDS = HEARTBEAT_INTERVAL_SECONDS * LIVENESS_MULTIPLIER

tasks: dict[str, dict] = {}


class CallbackPayload(BaseModel):
    task_id: str
    status: str
    result: str | None = None
    error: str | None = None


class HeartbeatPayload(BaseModel):
    task_id: str


async def liveness_monitor(task_id: str):
    """Detect a stalled subagent from *missing heartbeats*, fast.

    Stage 03's watchdog can only fire at the absolute deadline: a subagent that
    dies at second 1 of a 60s budget isn't noticed for 59s. Here the orchestrator
    instead watches the gap since the last heartbeat. Detection time no longer
    depends on how long the task was *supposed* to take -- it depends only on
    the heartbeat cadence. That decoupling is the whole win.

    We keep the absolute deadline too, as a backstop for a subagent that keeps
    beating but never actually finishes.
    """
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)

        task = tasks.get(task_id)
        if task is None or task["status"] != "running":
            return  # terminal state reached elsewhere; stop monitoring

        now = time.monotonic()
        since_beat = now - task["last_heartbeat"]
        since_start = now - task["started_at"]

        if since_beat > HEARTBEAT_DEADLINE_SECONDS:
            print(
                f"[Orchestrator] DEAD: task {task_id} missed heartbeats "
                f"({since_beat:.1f}s since last beat)"
            )
            task.update(
                status="failed",
                error=f"missed heartbeats ({since_beat:.1f}s silent)",
            )
            return

        if since_start > TASK_TIMEOUT_SECONDS:
            print(f"[Orchestrator] TIMEOUT: task {task_id} exceeded absolute deadline")
            task.update(status="timed_out", error="exceeded absolute deadline")
            return


@app.get("/start")
async def start_task(fail: bool = False, hang: bool = False, stall: bool = False):
    """Flags exercise each mode:
      ?fail   -> failure callback
      ?hang   -> hangs BEFORE beating: liveness_monitor catches it fast
      ?stall  -> beats a few times then goes silent mid-work
    """
    task_id = str(uuid.uuid4())
    now = time.monotonic()
    tasks[task_id] = {
        "status": "running",
        "result": None,
        "error": None,
        "started_at": now,
        "last_heartbeat": now,  # grace: first beat expected within a deadline
    }

    print(f"[Orchestrator] Starting task: {task_id} "
          f"(fail={fail}, hang={hang}, stall={stall})")

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{SUBAGENT_URL}/execute",
            json={
                "task_id": task_id,
                "callback_url": "http://localhost:8000/callback",
                "heartbeat_url": "http://localhost:8000/heartbeat",
                "heartbeat_interval": HEARTBEAT_INTERVAL_SECONDS,
                "should_fail": fail,
                "should_hang": hang,
                "should_stall": stall,
            },
        )

    print(f"[Orchestrator] Subagent response: {response.json()}")
    asyncio.create_task(liveness_monitor(task_id))
    return {"task_id": task_id, "status": "subagent_started"}


@app.post("/heartbeat")
async def receive_heartbeat(payload: HeartbeatPayload):
    task = tasks.get(payload.task_id)
    if task and task["status"] == "running":
        task["last_heartbeat"] = time.monotonic()
        print(f"[Orchestrator] heartbeat <- {payload.task_id}")
    return {"received": True}


@app.post("/callback")
async def receive_callback(payload: CallbackPayload):
    task = tasks.get(payload.task_id)
    if task is None:
        tasks[payload.task_id] = {"status": payload.status}
        return {"received": True, "task_id": payload.task_id}

    if task["status"] != "running":
        print(f"[Orchestrator] Ignoring late callback for {payload.task_id}: "
              f"already {task['status']}")
        return {"received": True, "task_id": payload.task_id, "ignored": True}

    print(f"[Orchestrator] CALLBACK <- {payload.task_id}: {payload.status}")
    task.update(status=payload.status, result=payload.result, error=payload.error)
    return {"received": True, "task_id": payload.task_id}


@app.get("/status/{task_id}")
async def get_status(task_id: str):
    return tasks.get(task_id, {"status": "unknown"})


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
