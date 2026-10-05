"""Deterministic synthetic source data.

Everything here is invented: names are combined from short lists, every email uses the
reserved ``example.com`` domain, and IDs are sequential. Nothing represents a real person.

Two snapshots simulate an upstream system over time:
  * batch 1 - history from 2025-01-01 to 2025-06-30 (initial load)
  * batch 2 - the same export two weeks later: new customers/orders, pending orders that
              completed, refunds, price changes, a few changed customer details.
Both are *full snapshots* (like a nightly export), which is exactly why the pipeline's
watermark filter matters: batch 2 contains every batch-1 row again.

A small, fixed set of defects is injected so validation and quarantine have real work to
do (bad emails, unknown customers, negative quantities, invalid payment methods, ...).
The same seed always produces byte-identical files.
"""

from __future__ import annotations

import csv
import json
import logging
import random
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

FIRST_NAMES = [
    "Avery",
    "Jordan",
    "Riley",
    "Casey",
    "Morgan",
    "Taylor",
    "Quinn",
    "Rowan",
    "Skyler",
    "Emerson",
    "Harper",
    "Reese",
    "Parker",
    "Sage",
    "Dakota",
    "Finley",
    "Hayden",
    "Kai",
    "Logan",
    "Marlow",
    "Noel",
    "Oakley",
    "Peyton",
    "Remy",
]
LAST_NAMES = [
    "Alder",
    "Birch",
    "Cedar",
    "Dale",
    "Ellis",
    "Frost",
    "Grove",
    "Hale",
    "Irving",
    "Jensen",
    "Keller",
    "Lane",
    "Mercer",
    "North",
    "Oakes",
    "Pryce",
    "Quill",
    "Reed",
    "Stone",
    "Thorne",
    "Vale",
    "Wells",
    "York",
    "Zane",
]
CITIES = {
    "US": ["Atlanta", "Austin", "Denver", "Seattle", "Chicago", "Boston"],
    "CA": ["Toronto", "Vancouver", "Montreal"],
    "GB": ["London", "Manchester", "Leeds"],
    "DE": ["Berlin", "Munich", "Hamburg"],
    "AU": ["Sydney", "Melbourne"],
    "IN": ["Bengaluru", "Pune", "Hyderabad"],
}
COUNTRY_WEIGHTS = {"US": 45, "CA": 12, "GB": 15, "DE": 10, "AU": 8, "IN": 10}
CATALOG = {
    "electronics": [
        ("Wireless Mouse", 24.99),
        ("USB-C Hub", 39.99),
        ("Mechanical Keyboard", 89.0),
        ("Noise-Cancelling Headphones", 179.0),
        ("27in Monitor", 249.0),
        ("Webcam HD", 59.5),
        ("Portable SSD 1TB", 109.0),
        ("Smart Speaker", 79.0),
    ],
    "home office": [
        ("Ergonomic Chair", 299.0),
        ("Standing Desk", 449.0),
        ("Desk Lamp", 34.5),
        ("Monitor Arm", 89.99),
        ("Cable Organizer", 12.99),
        ("Footrest", 29.0),
        ("Whiteboard", 49.0),
        ("Desk Mat", 19.99),
    ],
    "kitchen": [
        ("Coffee Grinder", 69.0),
        ("French Press", 29.99),
        ("Electric Kettle", 44.0),
        ("Chef Knife", 79.5),
        ("Cutting Board", 24.0),
        ("Espresso Cups Set", 19.5),
        ("Blender", 99.0),
        ("Food Storage Set", 32.0),
    ],
    "outdoor": [
        ("Hiking Backpack", 119.0),
        ("Water Bottle", 22.0),
        ("Camping Lantern", 36.0),
        ("Trekking Poles", 64.0),
        ("Rain Jacket", 139.0),
        ("Picnic Blanket", 27.5),
        ("Travel Hammock", 54.0),
        ("Sun Hat", 18.0),
    ],
    "books": [
        ("Data Engineering Basics", 39.0),
        ("SQL Pocket Guide", 24.99),
        ("Python Cookbook", 49.99),
        ("Cloud Patterns", 44.0),
        ("Statistics Primer", 29.0),
        ("Systems Design Notes", 54.0),
        ("Clean Pipelines", 34.0),
        ("Visualising Data", 42.0),
    ],
}
STATUS_WEIGHTS = {"completed": 60, "shipped": 15, "pending": 10, "cancelled": 10, "refunded": 5}
METHOD_WEIGHTS = {
    "credit_card": 50,
    "debit_card": 20,
    "paypal": 18,
    "bank_transfer": 7,
    "gift_card": 5,
}

BATCH1_START = datetime(2025, 1, 1, tzinfo=UTC)
BATCH1_END = datetime(2025, 6, 30, 23, 0, tzinfo=UTC)
BATCH2_START = datetime(2025, 7, 1, tzinfo=UTC)
BATCH2_END = datetime(2025, 7, 14, 23, 0, tzinfo=UTC)

CUSTOMER_HEADER = [
    "Customer ID",
    "First Name",
    "Last Name",
    "Email",
    "Country",
    "City",
    "Signup Date",
    "Updated At",
]  # messy on purpose: exercises column cleaning


def _ts(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Snapshot:
    customers: list[dict[str, Any]] = field(default_factory=list)
    products: list[dict[str, Any]] = field(default_factory=list)
    orders: list[dict[str, Any]] = field(default_factory=list)
    order_items: list[dict[str, Any]] = field(default_factory=list)
    payments: list[dict[str, Any]] = field(default_factory=list)


class SampleDataGenerator:
    def __init__(self, seed: int = 42) -> None:
        self.rng = random.Random(seed)
        self.snap = Snapshot()
        self._item_seq = 0
        self._payment_seq = 0

    # ------------------------------------------------------------------ helpers
    def _pick(self, weights: dict[str, int]) -> str:
        return self.rng.choices(list(weights), weights=list(weights.values()))[0]

    def _between(self, start: datetime, end: datetime) -> datetime:
        seconds = int((end - start).total_seconds())
        return (start + timedelta(seconds=self.rng.randint(0, seconds))).replace(second=0)

    def _next_item_id(self) -> str:
        self._item_seq += 1
        return f"OI{self._item_seq:07d}"

    def _next_payment_id(self) -> str:
        self._payment_seq += 1
        return f"PAY{self._payment_seq:07d}"

    # ------------------------------------------------------------------ entities
    def _customer(self, number: int, start: datetime, end: datetime) -> dict[str, Any]:
        first, last = self.rng.choice(FIRST_NAMES), self.rng.choice(LAST_NAMES)
        country = self._pick(COUNTRY_WEIGHTS)
        signup = self._between(start, end)
        return {
            "customer_id": f"C{number:05d}",
            "first_name": first,
            "last_name": last,
            "email": f"{first}.{last}{number}@example.com",  # mixed case on purpose
            "country": country.lower() if number % 17 == 0 else country,
            "city": self.rng.choice(CITIES[country]) if number % 23 else "",
            "signup_date": signup.date().isoformat(),
            "updated_at": _ts(signup),
        }

    def _products(self) -> None:
        number = 0
        for category, items in CATALOG.items():
            for name, price in items:
                number += 1
                self.snap.products.append(
                    {
                        "product_id": f"P{number:04d}",
                        "product_name": name,
                        "category": category,
                        "unit_price": f"{price:.2f}",
                        "is_active": "true" if number % 13 else "false",
                        "updated_at": _ts(
                            BATCH1_START - timedelta(days=30) + timedelta(hours=number)
                        ),
                    }
                )

    def _order(
        self,
        number: int,
        customer: dict[str, Any],
        start: datetime,
        end: datetime,
        status: str | None = None,
    ) -> dict[str, Any]:
        signup = datetime.fromisoformat(customer["signup_date"]).replace(tzinfo=UTC)
        order_date = self._between(max(start, signup), end)
        status = status or self._pick(STATUS_WEIGHTS)
        updated = min(order_date + timedelta(hours=self.rng.randint(1, 72)), end)
        order = {
            "order_id": f"O{number:06d}",
            "customer_id": customer["customer_id"],
            "order_date": _ts(order_date),
            "status": status,
            "currency": "USD",
            "updated_at": _ts(max(updated, order_date)),
        }
        net = self._items_for(order)
        self._payments_for(order, net)
        return order

    def _items_for(self, order: dict[str, Any]) -> float:
        net_total = 0.0
        catalog = [p for p in self.snap.products[:40] if _is_clean_product(p)]
        products = self.rng.sample(catalog, k=self.rng.choices([1, 2, 3, 4], [40, 30, 20, 10])[0])
        for product in products:
            quantity = self.rng.choices([1, 2, 3, 5], [60, 25, 10, 5])[0]
            price = float(product["unit_price"])
            gross = round(quantity * price, 2)
            discount = (
                round(gross * self.rng.choice([0.05, 0.10, 0.15]), 2)
                if self.rng.random() < 0.25
                else 0.0
            )
            net_total += gross - discount
            self.snap.order_items.append(
                {
                    "order_item_id": self._next_item_id(),
                    "order_id": order["order_id"],
                    "product_id": product["product_id"],
                    "quantity": str(quantity),
                    "unit_price": f"{price:.2f}",
                    "discount_amount": f"{discount:.2f}",
                    "updated_at": order["updated_at"],
                }
            )
        return round(net_total, 2)

    def _payments_for(self, order: dict[str, Any], net: float) -> None:
        status = order["status"]
        if status == "pending":
            return
        paid_at = datetime.fromisoformat(order["order_date"].replace("Z", "+00:00")) + timedelta(
            minutes=self.rng.randint(1, 30)
        )
        method = self._pick(METHOD_WEIGHTS)
        if status == "cancelled" or self.rng.random() < 0.05:  # a declined attempt
            self._add_payment(order, paid_at, method, net, "failed")
            if status == "cancelled":
                return
            paid_at += timedelta(minutes=5)
        final_status = "refunded" if status == "refunded" else "captured"
        self._add_payment(order, paid_at, method, net, final_status)

    def _add_payment(
        self, order: dict[str, Any], paid_at: datetime, method: str, amount: float, status: str
    ) -> None:
        self.snap.payments.append(
            {
                "payment_id": self._next_payment_id(),
                "order_id": order["order_id"],
                "payment_date": _ts(paid_at),
                "payment_method": method,
                "amount": round(amount, 2),
                "status": status,
                "updated_at": _ts(paid_at),
            }
        )

    # ------------------------------------------------------------------ batches
    def build_batch1(self) -> Snapshot:
        self._products()
        self.snap.customers = [
            self._customer(
                n, BATCH1_START - timedelta(days=365), BATCH1_START + timedelta(days=150)
            )
            for n in range(1, 201)
        ]
        for n in range(1, 1201):
            self.snap.orders.append(
                self._order(n, self.rng.choice(self.snap.customers), BATCH1_START, BATCH1_END)
            )
        self._inject_batch1_defects()
        return self.snap

    def build_batch2(self) -> Snapshot:
        """Apply two weeks of changes on top of batch 1 (call build_batch1 first)."""
        changed = BATCH2_START + timedelta(days=1)
        # Changed customer details.
        for customer in self.snap.customers[30:40]:
            customer["city"] = self.rng.choice(CITIES.get(customer["country"].upper(), ["Austin"]))
            customer["updated_at"] = _ts(self._between(changed, BATCH2_END))
        # New customers.
        for n in range(201, 221):
            self.snap.customers.append(
                self._customer(n, BATCH2_START, BATCH2_END - timedelta(days=3))
            )
        # Price change on two products, two new products.
        for product in self.snap.products[:2]:
            product["unit_price"] = f"{float(product['unit_price']) * 1.1:.2f}"
            product["updated_at"] = _ts(changed)
        for n, (name, price) in enumerate([("Laptop Stand", 45.0), ("Travel Mug", 21.0)], start=41):
            self.snap.products.append(
                {
                    "product_id": f"P{n:04d}",
                    "product_name": name,
                    "category": "home office",
                    "unit_price": f"{price:.2f}",
                    "is_active": "true",
                    "updated_at": _ts(changed),
                }
            )
        # Pending orders that completed (new captured payment) and completed ones refunded.
        pending = [
            o
            for o in self.snap.orders
            if o["status"] == "pending" and "C999" not in o["customer_id"]
        ]
        for order in pending[:30]:
            order["status"] = "completed"
            order["updated_at"] = _ts(self._between(changed, BATCH2_END))
            net = sum(
                float(i["unit_price"]) * int(i["quantity"]) - float(i["discount_amount"])
                for i in self.snap.order_items
                if i["order_id"] == order["order_id"]
            )
            self._add_payment(
                order,
                datetime.fromisoformat(order["updated_at"].replace("Z", "+00:00")),
                "credit_card",
                round(net, 2),
                "captured",
            )
        completed = [o for o in self.snap.orders if o["status"] == "completed"]
        for order in completed[40:45]:
            order["status"] = "refunded"
            order["updated_at"] = _ts(self._between(changed, BATCH2_END))
            for payment in self.snap.payments:
                if payment["order_id"] == order["order_id"] and payment["status"] == "captured":
                    payment["status"] = "refunded"
                    payment["updated_at"] = order["updated_at"]
        # New orders.
        # Only well-formed customers place new orders (the defective ones stay quarantined).
        active = [c for c in self.snap.customers if _is_clean_customer(c)]
        for n in range(1201, 1351):
            self.snap.orders.append(
                self._order(n, self.rng.choice(active), BATCH2_START, BATCH2_END)
            )
        # Two fresh defects so batch 2 also exercises quarantine.
        self.snap.orders[-1]["customer_id"] = "C99998"
        bad_item = next(i for i in reversed(self.snap.order_items) if i["order_id"] == "O001349")
        bad_item["quantity"] = "-1"
        return self.snap

    def _inject_batch1_defects(self) -> None:
        c, o, i, p = (
            self.snap.customers,
            self.snap.orders,
            self.snap.order_items,
            self.snap.payments,
        )
        c[10]["email"] = "not-an-email"
        c[11]["email"] = "missing.at.example.com"
        c[12]["last_name"] = ""
        c[13]["signup_date"] = "2025-02-30"
        c[14]["first_name"] = "  " + c[14]["first_name"].lower() + "  "  # cleaned, not rejected
        c.append(dict(c[20]))  # exact duplicate row
        older = dict(c[21])  # conflicting older version of the same key -> superseded
        older["city"] = "Old City"
        older["updated_at"] = "2023-01-01T00:00:00Z"
        c.insert(21, older)

        self.snap.products[7]["unit_price"] = "-5.00"
        self.snap.products[8]["is_active"] = "maybe"

        for idx in (100, 200, 300):
            o[idx]["customer_id"] = "C99999"
        o[400]["order_date"] = "31/12/2025"
        o[401]["order_date"] = "2031-01-01T00:00:00Z"  # future date
        o[500]["status"] = "unknown"
        o.extend([dict(o[600]), dict(o[601])])

        for idx in (50, 150, 250):
            i[idx]["quantity"] = "-2"
        i[350]["product_id"] = "P9999"
        i[351]["product_id"] = "P9998"
        i[450]["discount_amount"] = f"{float(i[450]['unit_price']) * 10:.2f}"  # > gross
        i[550]["unit_price"] = "abc"

        p[20]["amount"] = -10.0
        p[30]["amount"] = -1.5
        p[40]["payment_method"] = "crypto"
        p[41]["payment_method"] = "cash_on_delivery"
        p[60]["order_id"] = "O999999"
        p.append(dict(p[70]))


def _is_clean_customer(customer: dict[str, Any]) -> bool:
    try:
        date.fromisoformat(customer["signup_date"])
    except ValueError:
        return False
    return bool(customer["last_name"]) and "@" in customer["email"]


def _is_clean_product(product: dict[str, Any]) -> bool:
    return float(product["unit_price"]) > 0 and product["is_active"] in ("true", "false")


def write_snapshot(snapshot: Snapshot, output_dir: Path) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    keys = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "city",
        "signup_date",
        "updated_at",
    ]
    _write_csv(
        output_dir / "customers.csv",
        CUSTOMER_HEADER,
        [[r[k] for k in keys] for r in snapshot.customers],
    )
    for name in ("products", "orders", "order_items"):
        rows: list[dict[str, Any]] = getattr(snapshot, name)
        _write_csv(output_dir / f"{name}.csv", list(rows[0]), [list(r.values()) for r in rows])
    (output_dir / "payments.json").write_text(
        json.dumps(snapshot.payments, indent=1), encoding="utf-8"
    )
    counts = {
        name: len(getattr(snapshot, name))
        for name in ("customers", "products", "orders", "order_items", "payments")
    }
    logger.info("Wrote sample data to %s: %s", output_dir, counts)
    return counts


def _write_csv(path: Path, header: list[str], rows: list[list[Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def generate(output_dir: Path, batch: int = 1, seed: int = 42) -> dict[str, int]:
    """Write snapshot ``batch`` (1 or 2) to ``output_dir``."""
    if batch not in (1, 2):
        raise ValueError("batch must be 1 or 2")
    generator = SampleDataGenerator(seed)
    snapshot = generator.build_batch1()
    if batch == 2:
        snapshot = generator.build_batch2()
    return write_snapshot(snapshot, output_dir)


def snapshot_date(batch: int) -> date:
    return (BATCH1_END if batch == 1 else BATCH2_END).date()
