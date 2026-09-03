`ThreadOps` now rejects a `search_enrich_concurrency` below 1 instead of
accepting it and hanging. `asyncio.Semaphore` rejects negative values but
accepts `0`, and a bound of `0` is never acquirable — every thread search would
have waited on it forever, raising nothing and logging nothing. Construction
now fails loudly with a `ValueError`.
