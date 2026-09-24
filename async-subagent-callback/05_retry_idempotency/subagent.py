import asyncio

import httpx
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

# Retry policy for the callback delivery. Exponential backoff with a cap; a
# finite number of attempts so a permanently-dead orchestrator doesn't retry forever.
MAX_CALLBACK_ATTEMPTS = 6
BASE_BACKOFF_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 8.0


class ExecuteRequest(BaseModel):
    task_id: str
    callback_url: str
    heartbeat_url: str
    heartbeat_interval: float = 2.0
    should_fail: bool = False


async def send_heartbeats(task_id, heartbeat_url, interval, stop: asyncio.Event):
    async with httpx.AsyncClient() as client:
        while not stop.is_set():
            try:
                await client.post(heartbeat_url, json={"task_id": task_id})
            except Exception:  # noqa: BLE001
                pass
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass


async def do_work(task_id: str, should_fail: bool) -> str:
    print(f"[Subagent] Working on task: {task_id}")
    await asyncio.sleep(5)
    if should_fail:
        raise ValueError("simulated task failure")
    return "Subagent finished successfully!"


async def send_callback_with_retry(callback_url: str, payload: dict) -> bool:
    """Deliver the terminal callback with bounded exponential backoff.

    The callback is just another network request, so it can fail even when the
    task itself succeeded. Without retries a single dropped POST would strand
    the orchestrator in "running" until its watchdog fires -- turning a healthy task
    into a spurious timeout. We retry on connection errors and 5xx/429.

    We do NOT retry on 4xx (other than 429): a 400/422 means the request is
    malformed and will fail identically forever. Retrying it is pure waste.

    Combined with the orchestrator's idempotent handler, retrying is safe: delivering
    the same terminal state twice is a no-op on the orchestrator.
    """
    async with httpx.AsyncClient() as client:
        for attempt in range(1, MAX_CALLBACK_ATTEMPTS + 1):
            try:
                resp = await client.post(callback_url, json=payload, timeout=5.0)
                if resp.status_code < 400:
                    print(f"[Subagent] callback delivered on attempt #{attempt}")
                    return True
                if resp.status_code < 500 and resp.status_code != 429:
                    print(f"[Subagent] callback got {resp.status_code} "
                          f"(non-retryable); giving up")
                    return False
                print(f"[Subagent] callback attempt #{attempt} -> "
                      f"{resp.status_code} (retryable)")
            except httpx.HTTPError as exc:
                print(f"[Subagent] callback attempt #{attempt} failed: {exc!r}")

            if attempt < MAX_CALLBACK_ATTEMPTS:
                backoff = min(BASE_BACKOFF_SECONDS * 2 ** (attempt - 1),
                              MAX_BACKOFF_SECONDS)
                await asyncio.sleep(backoff)

    print("[Subagent] callback exhausted all retries; giving up")
    return False


async def run_task(req: ExecuteRequest):
    stop = asyncio.Event()
    hb = asyncio.create_task(
        send_heartbeats(req.task_id, req.heartbeat_url, req.heartbeat_interval, stop)
    )
    try:
        try:
            result = await do_work(req.task_id, req.should_fail)
            payload = {"task_id": req.task_id, "status": "completed",
                       "result": result}
        except Exception as exc:  # noqa: BLE001
            payload = {"task_id": req.task_id, "status": "failed",
                       "error": str(exc)}
        await send_callback_with_retry(req.callback_url, payload)
    finally:
        stop.set()
        hb.cancel()


@app.post("/execute")
async def execute(request: ExecuteRequest):
    print(f"[Subagent] Received task: {request.task_id}")
    asyncio.create_task(run_task(request))
    return {"task_id": request.task_id, "status": "accepted"}


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8001)
