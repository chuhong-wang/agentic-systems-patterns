import asyncio

import httpx
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI()


class ExecuteRequest(BaseModel):
    task_id: str
    callback_url: str


async def run_task(task_id: str, callback_url: str):
    print(f"[Subagent] Started task: {task_id}")

    # Simulate long-running work
    await asyncio.sleep(5)

    result = "Subagent finished successfully!"

    print(f"[Subagent] Finished task: {task_id}")
    print(f"[Subagent] Sending callback to: {callback_url}")

    async with httpx.AsyncClient() as client:
        response = await client.post(
            callback_url,
            json={
                "task_id": task_id,
                "status": "completed",
                "result": result,
            },
        )

    print(
        f"[Subagent] Callback response: "
        f"{response.status_code}"
    )


@app.post("/execute")
async def execute(request: ExecuteRequest):
    print(f"[Subagent] Received task: {request.task_id}")

    # Start task in the background.
    # The HTTP request returns immediately.
    asyncio.create_task(
        run_task(
            request.task_id,
            request.callback_url,
        )
    )

    return {
        "task_id": request.task_id,
        "status": "accepted",
    }


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="localhost",
        port=8001,
    )