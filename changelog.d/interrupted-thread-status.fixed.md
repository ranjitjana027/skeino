A thread parked on an `interrupt()` now reports `status: "interrupted"` instead
of `"idle"`, matching LangGraph Platform. The status was written from the run's
outcome alone, and a run that ends waiting for a human decision ends
successfully — so a thread waiting on an approval was indistinguishable from one
with nothing pending. The status is now read from the checkpoint after the run
settles; an unreadable checkpoint still falls back to `"idle"`.
