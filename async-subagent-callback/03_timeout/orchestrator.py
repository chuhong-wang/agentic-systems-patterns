import asyncio
import time
import uuid

import httpx
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

SUBAGENT_URL = "http://localhost:8001"

# How long the orchestrator will wait for ANY terminal callback before giving up.
# This is the whole point of stage 03: the orchestrator no longer trusts the
# subagent to always report back.
TASK_TIMEOUT_SECONDS = 8.0


tasks: dict[str, dict] = {}


class CallbackPayload(BaseModel):
    task_id: str
    status: str  # "completed" | "failed"
    result: str | None = None
    error: str | None = None


async def watchdog(task_id: str):
    """Independent orchestrator-side timer.

    Stages 01-02 assumed the subagent would eventually send *some* callback.
    But a crashed / hung / network-partitioned subagent sends nothing, and the
    task would sit in "running" forever. The watchdog is the orchestrator's own clock:
    if no terminal state is reached within the deadline, the orchestrator declares
    the task timed out itself.

    Note the deliberate design choice: a late callback can still arrive after a
    timeout. We treat the FIRST terminal state as authoritative and ignore the
    rest (see receive_callback). Timeout is a decision, not a fact about the
    subagent -- the work may actually have succeeded but we can no longer trust
    the result in time.
    """
    await asyncio.sleep(TASK_TIMEOUT_SECONDS)

    task = tasks.get(task_id)
    if task is None:
        return

    if task["status"] == "running":
        print(f"[Orchestrator] TIMEOUT: task {task_id} exceeded {TASK_TIMEOUT_SECONDS}s")
        task.update(status="timed_out", error="no callback before deadline")


@app.get("/start")
async def start_task(fail: bool = False, hang: bool = False):
    """Start a subagent task.

    Query flags let us exercise each failure mode:
      ?fail=true  -> subagent raises, sends a failure callback (stage 02 path)
      ?hang=true  -> subagent never calls back  -> orchestrator watchdog fires
    """
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "status": "running",
        "result": None,
        "error": None,
        "started_at": time.monotonic(),
    }

    print(f"[Orchestrator] Starting task: {task_id} (fail={fail}, hang={hang})")

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{SUBAGENT_URL}/execute",
            json={
                "task_id": task_id,
                "callback_url": "http://localhost:8000/callback",
                "should_fail": fail,
                "should_hang": hang,
            },
        )

    print(f"[Orchestrator] Subagent response: {response.json()}")

    # Arm the watchdog regardless of what the subagent promised.
    asyncio.create_task(watchdog(task_id))

    return {"task_id": task_id, "status": "subagent_started"}


@app.post("/callback")
async def receive_callback(payload: CallbackPayload):
    task = tasks.get(payload.task_id)

    if task is None:
        print(f"[Orchestrator] WARNING: callback for unknown task {payload.task_id}")
        tasks[payload.task_id] = {"status": payload.status}
        return {"received": True, "task_id": payload.task_id}

    # First terminal state wins. A callback that arrives after a timeout is
    # ignored so the orchestrator's decision stays stable.
    if task["status"] != "running":
        print(
            f"[Orchestrator] Ignoring late callback for {payload.task_id}: "
            f"already {task['status']}"
        )
        return {"received": True, "task_id": payload.task_id, "ignored": True}

    print("\n================================")
    print("[Orchestrator] CALLBACK RECEIVED")
    print(f"[Orchestrator] Task ID: {payload.task_id}")
    print(f"[Orchestrator] Status: {payload.status}")
    print("================================\n")

    task.update(status=payload.status, result=payload.result, error=payload.error)
    return {"received": True, "task_id": payload.task_id}


@app.get("/status/{task_id}")
async def get_status(task_id: str):
    return tasks.get(task_id, {"status": "unknown"})


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
