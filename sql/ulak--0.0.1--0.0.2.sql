-- ulak 0.0.1 -> 0.0.2 upgrade migration
-- No schema changes between 0.0.1 and 0.0.2 (release metadata only); this
-- script exists so ALTER EXTENSION ulak UPDATE has a path from 0.0.1.

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION ulak UPDATE TO '0.0.2'" to load this file. \quit

SELECT 1;
