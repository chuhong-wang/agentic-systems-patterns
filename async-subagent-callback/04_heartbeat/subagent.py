import asyncio

import httpx
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class ExecuteRequest(BaseModel):
    task_id: str
    callback_url: str
    heartbeat_url: str
    heartbeat_interval: float = 2.0
    should_fail: bool = False
    should_hang: bool = False
    should_stall: bool = False


async def send_heartbeats(task_id: str, heartbeat_url: str, interval: float,
                          stop: asyncio.Event):
    """Ping the orchestrator on a fixed cadence until told to stop.

    This runs concurrently with the actual work. It is the subagent's promise
    of "I am still alive and making progress." When the work coroutine finishes
    (or we simulate a stall by cancelling this), the beats stop and the orchestrator's
    liveness_monitor notices the silence.
    """
    async with httpx.AsyncClient() as client:
        while not stop.is_set():
            try:
                await client.post(heartbeat_url, json={"task_id": task_id})
                print(f"[Subagent] heartbeat -> {task_id}")
            except Exception as exc:  # noqa: BLE001
                print(f"[Subagent] heartbeat failed: {exc!r}")
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass


async def do_work(task_id, should_fail, should_hang, should_stall,
                  heartbeat_task: asyncio.Task) -> str:
    print(f"[Subagent] Working on task: {task_id}")

    if should_hang:
        # Dies before any heartbeat could establish liveness.
        heartbeat_task.cancel()
        print(f"[Subagent] Task {task_id} hanging with no heartbeats")
        await asyncio.sleep(3600)

    if should_stall:
        # Beat for a bit, then go silent mid-work: kill the heartbeat loop but
        # keep "working" forever. This is the case a coarse timeout handles
        # slowly and the heartbeat monitor handles fast.
        await asyncio.sleep(5)
        print(f"[Subagent] Task {task_id} stalling (heartbeats stop now)")
        heartbeat_task.cancel()
        await asyncio.sleep(3600)

    await asyncio.sleep(5)
    if should_fail:
        raise ValueError("simulated task failure")
    return "Subagent finished successfully!"


async def send_callback(callback_url: str, payload: dict) -> None:
    async with httpx.AsyncClient() as client:
        response = await client.post(callback_url, json=payload)
    print(f"[Subagent] callback -> {payload['status']} ({response.status_code})")


async def run_task(req: ExecuteRequest):
    stop = asyncio.Event()
    heartbeat_task = asyncio.create_task(
        send_heartbeats(req.task_id, req.heartbeat_url, req.heartbeat_interval, stop)
    )
    try:
        result = await do_work(
            req.task_id, req.should_fail, req.should_hang, req.should_stall,
            heartbeat_task,
        )
        await send_callback(
            req.callback_url,
            {"task_id": req.task_id, "status": "completed", "result": result},
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[Subagent] Task {req.task_id} failed: {exc!r}")
        await send_callback(
            req.callback_url,
            {"task_id": req.task_id, "status": "failed", "error": str(exc)},
        )
    finally:
        stop.set()
        heartbeat_task.cancel()


@app.post("/execute")
async def execute(request: ExecuteRequest):
    print(f"[Subagent] Received task: {request.task_id}")
    asyncio.create_task(run_task(request))
    return {"task_id": request.task_id, "status": "accepted"}


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8001)
