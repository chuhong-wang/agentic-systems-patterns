import asyncio

import httpx
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class ExecuteRequest(BaseModel):
    task_id: str
    callback_url: str
    should_fail: bool = False


async def do_work(task_id: str, should_fail: bool) -> str:
    """The actual task. May raise."""
    print(f"[Subagent] Working on task: {task_id}")
    await asyncio.sleep(5)  # simulate long-running work

    if should_fail:
        raise ValueError("simulated task failure")

    return "Subagent finished successfully!"


async def send_callback(callback_url: str, payload: dict) -> None:
    print(f"[Subagent] Sending callback to {callback_url}: {payload['status']}")
    async with httpx.AsyncClient() as client:
        response = await client.post(callback_url, json=payload)
    print(f"[Subagent] Callback response: {response.status_code}")


async def run_task(task_id: str, callback_url: str, should_fail: bool):
    """Run the task and ALWAYS report a terminal outcome back to the orchestrator.

    Stage 02's core idea: wrap the work in try/except so that a failure is a
    *signal we send*, not a silent death. In stage 01, an exception here would
    kill the background task and leave the orchestrator's task stuck in "running"
    forever. Here, success and failure both produce a callback.
    """
    try:
        result = await do_work(task_id, should_fail)
        await send_callback(
            callback_url,
            {"task_id": task_id, "status": "completed", "result": result},
        )
    except Exception as exc:  # noqa: BLE001 - we deliberately catch everything
        print(f"[Subagent] Task {task_id} failed: {exc!r}")
        await send_callback(
            callback_url,
            {"task_id": task_id, "status": "failed", "error": str(exc)},
        )


@app.post("/execute")
async def execute(request: ExecuteRequest):
    print(f"[Subagent] Received task: {request.task_id}")

    # Fire-and-forget: the HTTP response returns immediately while work runs.
    asyncio.create_task(
        run_task(
            request.task_id,
            request.callback_url,
            request.should_fail,
        )
    )

    return {"task_id": request.task_id, "status": "accepted"}


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8001)
