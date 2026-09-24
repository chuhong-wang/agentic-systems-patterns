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
    should_hang: bool = False


async def do_work(task_id: str, should_fail: bool, should_hang: bool) -> str:
    print(f"[Subagent] Working on task: {task_id}")

    if should_hang:
        # Simulate a crash / hang: the subagent stops making progress and
        # never reaches the callback. From the orchestrator's side this is
        # indistinguishable from the process dying -- only the watchdog saves it.
        print(f"[Subagent] Task {task_id} is hanging forever (no callback)")
        await asyncio.sleep(3600)

    await asyncio.sleep(5)  # normal long-running work

    if should_fail:
        raise ValueError("simulated task failure")

    return "Subagent finished successfully!"


async def send_callback(callback_url: str, payload: dict) -> None:
    print(f"[Subagent] Sending callback: {payload['status']}")
    async with httpx.AsyncClient() as client:
        response = await client.post(callback_url, json=payload)
    print(f"[Subagent] Callback response: {response.status_code}")


async def run_task(task_id, callback_url, should_fail, should_hang):
    try:
        result = await do_work(task_id, should_fail, should_hang)
        await send_callback(
            callback_url,
            {"task_id": task_id, "status": "completed", "result": result},
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[Subagent] Task {task_id} failed: {exc!r}")
        await send_callback(
            callback_url,
            {"task_id": task_id, "status": "failed", "error": str(exc)},
        )


@app.post("/execute")
async def execute(request: ExecuteRequest):
    print(f"[Subagent] Received task: {request.task_id}")
    asyncio.create_task(
        run_task(
            request.task_id,
            request.callback_url,
            request.should_fail,
            request.should_hang,
        )
    )
    return {"task_id": request.task_id, "status": "accepted"}


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8001)
