"""HTTP client for paginated REST APIs, with timeouts and selective retries.

Retry policy (only for failures that can succeed on a second attempt):
  * retried:     connection errors, timeouts, HTTP 429 and 5xx
  * NOT retried: other 4xx (bad request, auth, not found) and malformed JSON -
                 repeating the same request would fail the same way, so we fail fast.
Backoff is exponential (backoff, 2x, 4x, ...) and honours a numeric ``Retry-After``.

The client knows nothing about payments: it is reused by any paginated endpoint, and a
real vendor API can replace the simulated one by changing API_BASE_URL (plus a small
adapter if the vendor's pagination envelope differs from ``ApiPage``).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

import requests
from pydantic import BaseModel, ValidationError

from sales_pipeline.utils.exceptions import ApiError

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
MAX_PAGES = 10_000  # hard stop in case an API keeps returning a next_page forever


class ApiPage(BaseModel):
    """Pagination envelope contract. If the API breaks this contract we fail the run
    (the whole response is unusable); individual bad *records* are quarantined later."""

    data: list[dict[str, Any]]
    page: int
    page_size: int
    total_records: int
    total_pages: int
    next_page: int | None = None


@dataclass(frozen=True)
class FetchedPage:
    page: ApiPage
    raw_body: bytes  # exact bytes received, persisted unmodified to the raw zone


class _RetryableError(Exception):
    def __init__(self, message: str, status_code: int | None = None, wait: float | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.wait = wait


class RestApiClient:
    def __init__(
        self,
        base_url: str,
        timeout_seconds: float = 10.0,
        max_retries: int = 3,
        backoff_seconds: float = 1.0,
        token: str | None = None,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.session = session or requests.Session()
        self._sleep = sleep  # injectable so tests run instantly and deterministically
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"

    # ------------------------------------------------------------------ public API
    def iter_pages(
        self, path: str, params: dict[str, Any] | None = None, page_size: int = 100
    ) -> Iterator[FetchedPage]:
        """Yield every page of a paginated endpoint, following ``next_page``."""
        page_number = 1
        for _ in range(MAX_PAGES):
            query = {**(params or {}), "page": page_number, "page_size": page_size}
            body, payload = self._get_json(path, query)
            try:
                page = ApiPage.model_validate(payload)
            except ValidationError as exc:
                raise ApiError(f"Unexpected response shape from {path}: {exc}") from exc
            logger.info(
                "Fetched %s page %d/%d (%d records)",
                path,
                page.page,
                page.total_pages,
                len(page.data),
            )
            yield FetchedPage(page=page, raw_body=body)
            if page.next_page is None:
                return
            if page.next_page <= page_number:
                raise ApiError(
                    f"API pagination did not advance (page {page_number} -> {page.next_page})"
                )
            page_number = page.next_page
        raise ApiError(f"Stopped after {MAX_PAGES} pages; the API pagination looks broken")

    # ------------------------------------------------------------------ internals
    def _get_json(self, path: str, params: dict[str, Any]) -> tuple[bytes, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        attempts = self.max_retries + 1
        for attempt in range(1, attempts + 1):
            try:
                return self._request_once(url, params)
            except _RetryableError as exc:
                if attempt == attempts:
                    raise ApiError(
                        f"GET {url} failed after {attempt} attempts: {exc}",
                        status_code=exc.status_code,
                        attempts=attempt,
                    ) from exc
                wait = (
                    exc.wait if exc.wait is not None else self.backoff_seconds * 2 ** (attempt - 1)
                )
                logger.warning(
                    "Transient API failure (attempt %d/%d): %s - retrying in %.1fs",
                    attempt,
                    attempts,
                    exc,
                    wait,
                )
                self._sleep(wait)
        raise AssertionError("unreachable")  # pragma: no cover

    def _request_once(self, url: str, params: dict[str, Any]) -> tuple[bytes, Any]:
        try:
            response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        except requests.Timeout as exc:
            raise _RetryableError(f"timeout after {self.timeout_seconds}s") from exc
        except requests.ConnectionError as exc:
            raise _RetryableError(f"connection error: {exc}") from exc

        if response.status_code in RETRYABLE_STATUS_CODES:
            raise _RetryableError(
                f"HTTP {response.status_code}",
                status_code=response.status_code,
                wait=_retry_after_seconds(response),
            )
        if response.status_code >= 400:
            raise ApiError(
                f"GET {url} returned HTTP {response.status_code} (not retryable): "
                f"{response.text[:200]}",
                status_code=response.status_code,
            )
        try:
            return response.content, response.json()
        except ValueError as exc:
            raise ApiError(f"GET {url} returned invalid JSON") from exc


def _retry_after_seconds(response: requests.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None  # HTTP-date form: fall back to our own backoff
