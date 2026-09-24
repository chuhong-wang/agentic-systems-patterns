import uuid

import httpx
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI()


SUBAGENT_URL = "http://localhost:8001"


class CallbackPayload(BaseModel):
    task_id: str
    status: str
    result: str


@app.get("/start")
async def start_task():
    task_id = str(uuid.uuid4())

    print(f"[Orchestrator] Starting task: {task_id}")

    # Tell the subagent to start working.
    # This request returns immediately after the subagent accepts the task.
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{SUBAGENT_URL}/execute",
            json={
                "task_id": task_id,
                "callback_url": "http://localhost:8000/callback",
            },
        )

    print(f"[Orchestrator] Subagent response: {response.json()}")

    return {
        "task_id": task_id,
        "status": "subagent_started",
    }


@app.post("/callback")
async def receive_callback(payload: CallbackPayload):
    print("\n================================")
    print("[Orchestrator] CALLBACK RECEIVED")
    print(f"[Orchestrator] Task ID: {payload.task_id}")
    print(f"[Orchestrator] Status: {payload.status}")
    print(f"[Orchestrator] Result: {payload.result}")
    print("================================\n")

    return {
        "received": True,
        "task_id": payload.task_id,
    }


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="localhost",
        port=8000,
    )