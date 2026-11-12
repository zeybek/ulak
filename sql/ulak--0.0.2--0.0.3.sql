-- ulak 0.0.2 -> 0.0.3 upgrade migration
-- No schema changes. The worker partition algorithm changed in C code:
-- ordering_key messages are now hash-partitioned by hashtext(ordering_key)
-- instead of id-based modulo, restoring per-key FIFO guarantee in
-- multi-worker deployments. Existing pending messages continue to be
-- fetched correctly under the new algorithm.

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION ulak UPDATE TO '0.0.3'" to load this file. \quit

SELECT 1;
