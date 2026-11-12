-- ulak 0.1.1 -> 0.1.2 upgrade migration
-- No schema changes between 0.1.1 and 0.1.2 (upgrade-path and CI fixes only);
-- this script exists so ALTER EXTENSION ulak UPDATE has a path from 0.1.1.

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION ulak UPDATE TO '0.1.2'" to load this file. \quit

SELECT 1;
