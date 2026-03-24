from __future__ import annotations

import pandas as pd

from src.transform.build_silver import build_customer_summary, build_order_facts


def test_build_order_facts_creates_delivery_and_discount_columns() -> None:
    orders = pd.DataFrame(
        [
            {
                "order_id": 1,
                "customer_id": 10,
                "product_id": 100,
                "quantity": 2,
                "order_timestamp": "2025-03-01T08:00:00",
                "shipping_date": "2025-03-03",
                "order_status": "delivered",
                "discount_pct": 0.10,
            }
        ]
    )
    customers = pd.DataFrame([{"customer_id": 10, "full_name": "Test User", "country": "Thailand"}])
    products = pd.DataFrame([{"product_id": 100, "product_name": "Keyboard", "category": "Electronics"}])
    payments = pd.DataFrame(
        [
            {
                "order_id": 1,
                "payment_id": "PAY-1",
                "payment_method": "card",
                "gross_amount": 200.0,
                "net_amount": 180.0,
                "payment_status": "paid",
            }
        ]
    )

    result = build_order_facts(orders, customers, products, payments)

    assert result.loc[0, "discount_amount"] == 20.0
    assert result.loc[0, "delivery_days"] == 2
    assert bool(result.loc[0, "is_returned"]) is False


def test_build_customer_summary_aggregates_paid_orders_only() -> None:
    order_facts = pd.DataFrame(
        [
            {"customer_id": 1, "full_name": "A", "country": "TH", "order_id": 1, "net_amount": 100.0, "payment_status": "paid", "order_date": "2025-03-01"},
            {"customer_id": 1, "full_name": "A", "country": "TH", "order_id": 2, "net_amount": 200.0, "payment_status": "paid", "order_date": "2025-03-05"},
            {"customer_id": 1, "full_name": "A", "country": "TH", "order_id": 3, "net_amount": 300.0, "payment_status": "pending", "order_date": "2025-03-06"},
        ]
    )

    result = build_customer_summary(order_facts)

    assert result.loc[0, "total_orders"] == 2
    assert result.loc[0, "total_revenue"] == 300.0
    assert result.loc[0, "avg_order_value"] == 150.0
