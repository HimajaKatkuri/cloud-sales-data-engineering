"""Exception hierarchy.

One base class lets the CLI distinguish "expected" pipeline failures (clean error message,
non-zero exit code) from programming bugs (full traceback). Each subclass maps to a stage
so logs and the run-audit table say *where* a run failed.
"""


class PipelineError(Exception):
    """Base class for every failure the pipeline raises deliberately."""


class ConfigurationError(PipelineError):
    """Missing or invalid configuration. Never retried: it will not fix itself."""


class ExtractionError(PipelineError):
    """A source could not be read."""


class ApiError(ExtractionError):
    """The REST API returned an error or could not be reached after all retries."""

    def __init__(self, message: str, status_code: int | None = None, attempts: int = 1) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.attempts = attempts


class StorageError(PipelineError):
    """Reading or writing the raw/processed/rejected zones failed (local disk or S3)."""


class SchemaValidationError(PipelineError):
    """A whole feed is structurally unusable (e.g. a required column is missing).

    Row-level problems are *not* raised; those rows are quarantined instead.
    """


class DataQualityError(PipelineError):
    """One or more critical data-quality checks failed; the load is rolled back."""


class LoadError(PipelineError):
    """Writing to PostgreSQL failed."""
