"""Shared test fixtures. Everything here is deterministic: fixed timestamps, fixed seeds,
no network, no wall-clock dependence."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

AS_OF = datetime(2025, 7, 15, tzinfo=UTC)


@pytest.fixture
def as_of() -> datetime:
    return AS_OF


@pytest.fixture
def customer_rows() -> list[dict[str, object]]:
    return [
        {
            "customer_id": "C00001",
            "first_name": "Avery",
            "last_name": "Stone",
            "email": "avery.stone1@example.com",
            "country": "US",
            "city": "Austin",
            "signup_date": "2024-05-01",
            "updated_at": "2025-01-01T10:00:00Z",
        },
        {
            "customer_id": "C00002",
            "first_name": "Riley",
            "last_name": "Vale",
            "email": "riley.vale2@example.com",
            "country": "gb",
            "city": None,
            "signup_date": "2024-06-01",
            "updated_at": "2025-01-02T10:00:00Z",
        },
    ]


@pytest.fixture
def order_rows() -> list[dict[str, object]]:
    return [
        {
            "order_id": "O000001",
            "customer_id": "C00001",
            "order_date": "2025-03-01T09:00:00Z",
            "status": "completed",
            "currency": "USD",
            "updated_at": "2025-03-01T12:00:00Z",
        },
        {
            "order_id": "O000002",
            "customer_id": "C00002",
            "order_date": "2025-03-01T15:00:00Z",
            "status": "shipped",
            "currency": "USD",
            "updated_at": "2025-03-02T12:00:00Z",
        },
        {
            "order_id": "O000003",
            "customer_id": "C00001",
            "order_date": "2025-03-02T09:00:00Z",
            "status": "cancelled",
            "currency": "USD",
            "updated_at": "2025-03-02T12:00:00Z",
        },
    ]


@pytest.fixture
def item_rows() -> list[dict[str, object]]:
    return [
        {
            "order_item_id": "OI0000001",
            "order_id": "O000001",
            "product_id": "P0001",
            "quantity": "2",
            "unit_price": "10.00",
            "discount_amount": "2.00",
            "updated_at": "2025-03-01T12:00:00Z",
        },
        {
            "order_item_id": "OI0000002",
            "order_id": "O000001",
            "product_id": "P0002",
            "quantity": "1",
            "unit_price": "5.50",
            "discount_amount": "0.00",
            "updated_at": "2025-03-01T12:00:00Z",
        },
        {
            "order_item_id": "OI0000003",
            "order_id": "O000002",
            "product_id": "P0001",
            "quantity": "3",
            "unit_price": "10.00",
            "discount_amount": "0.00",
            "updated_at": "2025-03-02T12:00:00Z",
        },
        {
            "order_item_id": "OI0000004",
            "order_id": "O000003",
            "product_id": "P0002",
            "quantity": "4",
            "unit_price": "5.50",
            "discount_amount": "0.00",
            "updated_at": "2025-03-02T12:00:00Z",
        },
    ]
