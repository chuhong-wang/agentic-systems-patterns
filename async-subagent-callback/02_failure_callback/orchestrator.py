import uuid

import httpx
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

SUBAGENT_URL = "http://localhost:8001"


# In-memory task registry.
# Stage 02 change vs stage 01: the orchestrator now actually records task state,
# so a callback has somewhere to land. A task moves:
#   running -> completed   (success callback)
#   running -> failed      (failure callback)
tasks: dict[str, dict] = {}


class CallbackPayload(BaseModel):
    task_id: str
    status: str  # "completed" | "failed"
    result: str | None = None
    error: str | None = None


@app.get("/start")
async def start_task(fail: bool = False):
    """Start a subagent task.

    Pass ?fail=true to make the subagent raise mid-work, so we can watch
    the failure callback path instead of the success path.
    """
    task_id = str(uuid.uuid4())
    tasks[task_id] = {"status": "running", "result": None, "error": None}

    print(f"[Orchestrator] Starting task: {task_id} (fail={fail})")

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{SUBAGENT_URL}/execute",
            json={
                "task_id": task_id,
                "callback_url": "http://localhost:8000/callback",
                "should_fail": fail,
            },
        )

    print(f"[Orchestrator] Subagent response: {response.json()}")

    return {"task_id": task_id, "status": "subagent_started"}


@app.post("/callback")
async def receive_callback(payload: CallbackPayload):
    print("\n================================")
    print("[Orchestrator] CALLBACK RECEIVED")
    print(f"[Orchestrator] Task ID: {payload.task_id}")
    print(f"[Orchestrator] Status: {payload.status}")
    print(f"[Orchestrator] Result: {payload.result}")
    print(f"[Orchestrator] Error:  {payload.error}")
    print("================================\n")

    task = tasks.get(payload.task_id)
    if task is None:
        # A callback for a task we don't know about. Record it defensively
        # rather than crashing; stage 05 revisits unknown/duplicate callbacks.
        print(f"[Orchestrator] WARNING: callback for unknown task {payload.task_id}")
        tasks[payload.task_id] = {"status": payload.status}

    tasks[payload.task_id].update(
        status=payload.status,
        result=payload.result,
        error=payload.error,
    )

    return {"received": True, "task_id": payload.task_id}


@app.get("/status/{task_id}")
async def get_status(task_id: str):
    """Let a caller poll where a task ended up."""
    return tasks.get(task_id, {"status": "unknown"})


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
