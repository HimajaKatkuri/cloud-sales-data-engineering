"""REST client: pagination, which failures are retried, backoff, and error surfacing.

The HTTP session is a fake, and ``sleep`` is replaced with a recorder, so the tests are
instant and deterministic.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
import requests

from sales_pipeline.ingestion.api_client import RestApiClient
from sales_pipeline.utils.exceptions import ApiError


class FakeResponse:
    def __init__(self, status: int, body: Any = None, headers: dict[str, str] | None = None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        self.content = json.dumps(body).encode() if body is not None else b"not json"
        self.text = self.content.decode()

    def json(self) -> Any:
        if self._body is None:
            raise ValueError("no json")
        return self._body


class FakeSession:
    """Returns (or raises) the scripted outcomes in order and records each call."""

    def __init__(self, outcomes: list[Any]):
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, params: dict[str, Any], timeout: float) -> FakeResponse:
        self.calls.append({"url": url, "params": dict(params), "timeout": timeout})
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def page(n: int, total_pages: int, records: list[dict[str, Any]]) -> FakeResponse:
    return FakeResponse(
        200,
        {
            "data": records,
            "page": n,
            "page_size": 2,
            "total_records": 5,
            "total_pages": total_pages,
            "next_page": n + 1 if n < total_pages else None,
        },
    )


def make_client(session: FakeSession, sleeps: list[float], retries: int = 3) -> RestApiClient:
    return RestApiClient(
        "http://api.test",
        timeout_seconds=5,
        max_retries=retries,
        backoff_seconds=1.0,
        session=session,  # type: ignore[arg-type]
        sleep=sleeps.append,
    )


def test_follows_pagination_until_next_page_is_null():
    session = FakeSession(
        [
            page(1, 3, [{"id": 1}, {"id": 2}]),
            page(2, 3, [{"id": 3}, {"id": 4}]),
            page(3, 3, [{"id": 5}]),
        ]
    )
    client = make_client(session, [])

    pages = list(client.iter_pages("/payments", params={"updated_since": "x"}, page_size=2))

    assert [r["id"] for p in pages for r in p.page.data] == [1, 2, 3, 4, 5]
    assert [c["params"]["page"] for c in session.calls] == [1, 2, 3]
    assert all(c["params"]["updated_since"] == "x" for c in session.calls)
    assert all(c["timeout"] == 5 for c in session.calls)


def test_raw_body_is_kept_byte_for_byte():
    response = page(1, 1, [{"id": 1}])
    client = make_client(FakeSession([response]), [])

    fetched = next(client.iter_pages("/payments"))

    assert fetched.raw_body == response.content


@pytest.mark.parametrize(
    "transient",
    [
        requests.Timeout("read timed out"),
        requests.ConnectionError("connection refused"),
        FakeResponse(503),
        FakeResponse(500),
        FakeResponse(429),
    ],
    ids=["timeout", "connection-error", "503", "500", "429"],
)
def test_transient_failures_are_retried_then_succeed(transient):
    sleeps: list[float] = []
    session = FakeSession([transient, transient, page(1, 1, [{"id": 1}])])

    pages = list(make_client(session, sleeps).iter_pages("/payments"))

    assert len(pages) == 1
    assert len(session.calls) == 3
    assert sleeps == [1.0, 2.0]  # exponential backoff


def test_gives_up_after_max_retries_with_clear_error():
    sleeps: list[float] = []
    session = FakeSession([FakeResponse(503)] * 4)

    with pytest.raises(ApiError) as exc_info:
        list(make_client(session, sleeps, retries=3).iter_pages("/payments"))

    assert exc_info.value.attempts == 4
    assert exc_info.value.status_code == 503
    assert sleeps == [1.0, 2.0, 4.0]


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_client_errors_are_not_retried(status):
    sleeps: list[float] = []
    session = FakeSession([FakeResponse(status, {"error": "nope"})])

    with pytest.raises(ApiError) as exc_info:
        list(make_client(session, sleeps).iter_pages("/payments"))

    assert exc_info.value.status_code == status
    assert len(session.calls) == 1
    assert sleeps == []


def test_retry_after_header_is_honoured():
    sleeps: list[float] = []
    session = FakeSession(
        [FakeResponse(429, {"error": "slow down"}, {"Retry-After": "7"}), page(1, 1, [])]
    )

    list(make_client(session, sleeps).iter_pages("/payments"))

    assert sleeps == [7.0]


def test_invalid_json_is_not_retried():
    session = FakeSession([FakeResponse(200, None)])

    with pytest.raises(ApiError, match="invalid JSON"):
        list(make_client(session, []).iter_pages("/payments"))
    assert len(session.calls) == 1


def test_response_that_breaks_the_envelope_contract_fails():
    session = FakeSession([FakeResponse(200, {"items": []})])

    with pytest.raises(ApiError, match="Unexpected response shape"):
        list(make_client(session, []).iter_pages("/payments"))


def test_pagination_that_does_not_advance_is_detected():
    stuck = FakeResponse(
        200,
        {
            "data": [],
            "page": 1,
            "page_size": 2,
            "total_records": 9,
            "total_pages": 5,
            "next_page": 1,
        },
    )
    with pytest.raises(ApiError, match="did not advance"):
        list(make_client(FakeSession([stuck]), []).iter_pages("/payments"))


def test_bearer_token_is_sent_when_configured():
    session = FakeSession([])
    RestApiClient("http://api.test", token="t0k3n", session=session)  # type: ignore[arg-type]

    assert session.headers["Authorization"] == "Bearer t0k3n"
