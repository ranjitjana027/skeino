A paused run now reaches the client. The output-schema filter treated
LangGraph's reserved `__interrupt__` channel as ordinary graph state and
stripped it from `values` events, so a graph that called `interrupt()` looked to
the SDK like a run that simply stopped — no approval prompt, no way to resume.
Reserved dunder channels are now exempt from the filter in both `values` and
`updates` events; graph state is still filtered exactly as before.
