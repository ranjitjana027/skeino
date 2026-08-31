The Postgres checkpointer and metadata store now genuinely disable client-side
prepared statements behind a transaction-mode pooler. Both pools passed
`prepare_threshold=0`, which psycopg reads as *prepare on the first execution* —
the opposite of the intent — so a query could still be prepared and then fail
with `prepared statement "_pg3_0" does not exist` once pgbouncer or Supabase
routed a later call to a different server-side session. The value is now `None`,
which is what actually turns preparation off.
