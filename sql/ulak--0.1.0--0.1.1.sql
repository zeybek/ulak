-- ulak 0.1.0 -> 0.1.1 upgrade migration
-- No schema changes between 0.1.0 and 0.1.1 (Docker image fix only); this
-- script exists so ALTER EXTENSION ulak UPDATE has a path from 0.1.0.
SELECT 1;
