-- Operational metadata: run audit, incremental state, quarantine.

CREATE TABLE IF NOT EXISTS pipeline.pipeline_runs (
    batch_id       VARCHAR(40)  PRIMARY KEY,
    started_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    status         VARCHAR(10)  NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    full_refresh   BOOLEAN      NOT NULL DEFAULT FALSE,
    metrics        JSONB,
    error_message  TEXT
);
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_started ON pipeline.pipeline_runs (started_at DESC);

-- One row per entity: the highest source updated_at already processed.
CREATE TABLE IF NOT EXISTS pipeline.watermarks (
    entity          VARCHAR(30)  PRIMARY KEY,
    high_watermark  TIMESTAMPTZ  NOT NULL,
    last_batch_id   VARCHAR(40)  NOT NULL,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Quarantined rows, queryable with SQL (a copy also goes to the rejected/ storage zone).
CREATE TABLE IF NOT EXISTS pipeline.rejected_records (
    rejected_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id           VARCHAR(40)  NOT NULL,
    entity             VARCHAR(30)  NOT NULL,
    record             JSONB        NOT NULL,  -- original values exactly as received
    rejection_reasons  TEXT         NOT NULL,
    rejected_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rejected_batch  ON pipeline.rejected_records (batch_id);
CREATE INDEX IF NOT EXISTS ix_rejected_entity ON pipeline.rejected_records (entity);
