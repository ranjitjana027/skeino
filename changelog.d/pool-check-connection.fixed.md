Pooled metadata-store connections are no longer handed out inside an open
transaction. The pool's liveness probe ran `SELECT 1` on a connection that is
not in autocommit, which starts a transaction, so every checkout could arrive
`INTRANS`. Both the metadata store and the checkpointer now use
`AsyncConnectionPool.check_connection`, which toggles autocommit around the
probe so the connection comes back clean. A failure of the metadata-store index
advisory unlock also no longer masks the index-maintenance error that caused it.
