`POST /threads/{id}/runs/wait` and `GET /threads/{id}/runs/{run_id}/join` now
report a paused run. Their output was the checkpoint's state values alone, and
interrupts do not live there, so a run parked on `interrupt()` was
indistinguishable from a completed one — the awaiting tool call with no result
after it, and no `__interrupt__` to act on. Pending interrupts (from the
snapshot, or per task on older LangGraph) are now merged onto the reserved
`__interrupt__` channel, serialized exactly as the streaming path and LangGraph
Platform's `runs.wait` do.
