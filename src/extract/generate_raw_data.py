from __future__ import annotations

from datetime import datetime, timedelta
from random import Random

import pandas as pd

from src.common.config import RAW_DIR
from src.common.io import write_dataframe


RNG = Random(42)


def build_customers() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"customer_id": 1, "full_name": "Ava Thompson", "city": "Bangkok", "country": "Thailand", "signup_date": "2025-01-03"},
            {"customer_id": 2, "full_name": "Noah Carter", "city": "Singapore", "country": "Singapore", "signup_date": "2025-01-11"},
            {"customer_id": 3, "full_name": "Mia Johnson", "city": "Jakarta", "country": "Indonesia", "signup_date": "2025-02-14"},
            {"customer_id": 4, "full_name": "Liam Patel", "city": "Kuala Lumpur", "country": "Malaysia", "signup_date": "2025-02-28"},
            {"customer_id": 5, "full_name": "Sophia Nguyen", "city": "Ho Chi Minh City", "country": "Vietnam", "signup_date": "2025-03-06"},
            {"customer_id": 6, "full_name": "Ethan Lee", "city": "Manila", "country": "Philippines", "signup_date": "2025-03-18"},
        ]
    )


def build_products() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"product_id": 101, "product_name": "Noise Cancelling Headphones", "category": "Electronics", "unit_price": 120.0},
            {"product_id": 102, "product_name": "Mechanical Keyboard", "category": "Electronics", "unit_price": 89.0},
            {"product_id": 103, "product_name": "Standing Desk", "category": "Furniture", "unit_price": 310.0},
            {"product_id": 104, "product_name": "Ergonomic Chair", "category": "Furniture", "unit_price": 260.0},
            {"product_id": 105, "product_name": "Fitness Watch", "category": "Wearables", "unit_price": 150.0},
            {"product_id": 106, "product_name": "Portable SSD", "category": "Accessories", "unit_price": 99.0},
        ]
    )


def build_orders() -> pd.DataFrame:
    base_date = datetime(2025, 3, 1)
    rows = []
    for order_id in range(1001, 1025):
        customer_id = RNG.randint(1, 6)
        product_id = RNG.randint(101, 106)
        quantity = RNG.randint(1, 4)
        order_ts = base_date + timedelta(days=RNG.randint(0, 20), hours=RNG.randint(0, 23))
        shipping_days = RNG.randint(1, 5)
        rows.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "product_id": product_id,
                "quantity": quantity,
                "order_timestamp": order_ts.isoformat(),
                "shipping_date": (order_ts + timedelta(days=shipping_days)).date().isoformat(),
                "order_status": RNG.choice(["delivered", "delivered", "delivered", "processing", "returned"]),
                "discount_pct": RNG.choice([0.0, 0.0, 0.05, 0.10]),
            }
        )
    return pd.DataFrame(rows)


def build_payments(orders: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    product_price_map = products.set_index("product_id")["unit_price"].to_dict()
    rows = []
    payment_methods = ["card", "wallet", "bank_transfer"]
    for _, order in orders.iterrows():
        gross_amount = product_price_map[order["product_id"]] * order["quantity"]
        net_amount = gross_amount * (1 - order["discount_pct"])
        rows.append(
            {
                "payment_id": f"PAY-{order['order_id']}",
                "order_id": order["order_id"],
                "payment_method": RNG.choice(payment_methods),
                "gross_amount": round(gross_amount, 2),
                "net_amount": round(net_amount, 2),
                "payment_status": "paid" if order["order_status"] != "processing" else "pending",
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    customers = build_customers()
    products = build_products()
    orders = build_orders()
    payments = build_payments(orders, products)

    write_dataframe(customers, RAW_DIR / "customers.csv")
    write_dataframe(products, RAW_DIR / "products.csv")
    write_dataframe(orders, RAW_DIR / "orders.csv")
    write_dataframe(payments, RAW_DIR / "payments.csv")

    print(f"Generated raw data in {RAW_DIR}")


if __name__ == "__main__":
    main()

