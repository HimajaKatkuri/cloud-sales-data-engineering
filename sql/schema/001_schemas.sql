-- Three schemas separate responsibilities:
--   core      : cleaned, validated operational entities (one table per source entity)
--   analytics : star schema for reporting (dimensions + facts)
--   pipeline  : run audit, incremental watermarks, quarantined records
-- All DDL is idempotent (IF NOT EXISTS) so `sales-pipeline init-db` can run on every deploy.

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS pipeline;
