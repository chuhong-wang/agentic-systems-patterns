# Async subagents: from callback to reliable completion signals

## Brief
When an orchestrator delegates a long-running task to a subagent, a simple synchronous request/response model doesn't work well as we don't want the orchestor to be blocked by this task. Async/await only works when orchestrator and the subagents are on the same machine and the process return only at the completion. The doesn't work in the case of remote container or value returned at event trigger. Instead, the subagent should be able to execute independently and notify the orchestrator when the work is complete. 

I started with a very simple implementation of callback using FastAPI and then gradually added more layers to handle different failure mode at various points of the lifetime. 

## 1. callback with POST + GET, cannot handle subagent half-way failure 
Orchestror exposes two endpoints:
- `GET /start` -> starts a subagent task
- `POST /callback` -> receives the subagent's result (along with a signal at completion)

Subagent exposes one endpoint:
- `POST /execute` -> accepts the task from Orchestrator, async does work, call orchestrator's webhook when done 

```
Orchestrator
    │
    │ POST /execute
    ▼
Subagent
    │
    │ async work
    │
    │ POST /callback
    ▼
Orchestrator
```
the key point: This solves the basic asynchronous communication problem, but it assumes that the subagent will eventually send a callback.

### Orchestrator in-memory task registry 
```
tasks = {
    "task-123": {
        "status": "running"
    }
}
```

## 2. Add explicit failure callback 
```
Subagent
  ├── success → callback(status=completed)
  └── exception → callback(status=failed)
```

## 3. Add parent timeout to detect hang


## 4. Add heartbeat to quickly detect hang

## 5. Add callback retries to handle failure at the point of callback 



