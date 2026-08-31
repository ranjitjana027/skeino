The Postgres metadata store now runs over a shared `AsyncConnectionPool` instead
of opening a fresh connection for every operation. A connect + TLS handshake +
SCRAM exchange per query dominated request latency against a managed Postgres in
another region, and the cost scaled with the number of queries a request made.
Connections are validated before checkout and prepared statements are disabled,
so the store also stays correct behind a transaction-mode pooler.
