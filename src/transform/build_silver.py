from __future__ import annotations

import pandas as pd

from src.common.config import PROCESSED_DIR, RAW_DIR
from src.common.io import read_dataframe, write_dataframe


def standardize_customers(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned["signup_date"] = pd.to_datetime(cleaned["signup_date"])
    cleaned["full_name"] = cleaned["full_name"].str.strip()
    return cleaned


def standardize_products(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned["unit_price"] = cleaned["unit_price"].astype(float)
    return cleaned


def build_order_facts(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    payments: pd.DataFrame,
) -> pd.DataFrame:
    merged = (
        orders.merge(customers, on="customer_id", how="left")
        .merge(products, on="product_id", how="left")
        .merge(payments, on="order_id", how="left")
    )
    merged["order_timestamp"] = pd.to_datetime(merged["order_timestamp"])
    merged["shipping_date"] = pd.to_datetime(merged["shipping_date"])
    merged["discount_amount"] = (merged["gross_amount"] - merged["net_amount"]).round(2)
    merged["delivery_days"] = (merged["shipping_date"] - merged["order_timestamp"].dt.normalize()).dt.days
    merged["is_returned"] = merged["order_status"].eq("returned")
    merged["order_date"] = merged["order_timestamp"].dt.date
    merged["order_month"] = merged["order_timestamp"].dt.to_period("M").astype(str)
    return merged


def build_customer_summary(order_facts: pd.DataFrame) -> pd.DataFrame:
    delivered = order_facts.loc[order_facts["payment_status"].eq("paid")]
    summary = (
        delivered.groupby(["customer_id", "full_name", "country"], as_index=False)
        .agg(
            total_orders=("order_id", "nunique"),
            total_revenue=("net_amount", "sum"),
            avg_order_value=("net_amount", "mean"),
            first_order_date=("order_date", "min"),
            last_order_date=("order_date", "max"),
        )
        .sort_values("total_revenue", ascending=False)
    )
    summary["total_revenue"] = summary["total_revenue"].round(2)
    summary["avg_order_value"] = summary["avg_order_value"].round(2)
    return summary


def run() -> None:
    customers = standardize_customers(read_dataframe(RAW_DIR / "customers.csv"))
    products = standardize_products(read_dataframe(RAW_DIR / "products.csv"))
    orders = read_dataframe(RAW_DIR / "orders.csv")
    payments = read_dataframe(RAW_DIR / "payments.csv")

    order_facts = build_order_facts(orders, customers, products, payments)
    customer_summary = build_customer_summary(order_facts)

    write_dataframe(customers, PROCESSED_DIR / "silver_customers.csv")
    write_dataframe(products, PROCESSED_DIR / "silver_products.csv")
    write_dataframe(order_facts, PROCESSED_DIR / "silver_order_facts.csv")
    write_dataframe(customer_summary, PROCESSED_DIR / "silver_customer_summary.csv")

    print(f"Built silver datasets in {PROCESSED_DIR}")


if __name__ == "__main__":
    run()

