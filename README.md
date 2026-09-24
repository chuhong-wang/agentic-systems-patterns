This repository contains minimal, standalone implementations of infrastructure patterns used in agentic AI systems.

The examples are intentionally simplified and are not the production implementation of any specific system. They are designed to isolate individual architectural problems and make the underlying design tradeoffs clear.

| Component | Problem | Status |
|---|---|---|
| [subagent callback](./async-subagent-callback) | subagent completion signal + reliability | done |
| agent-managed sandboxes | dynamic resources allocation | TODO |
| failure Recovery | layers of Fault tolerance | TODO |
| artifact Registry | structured output | TODO |