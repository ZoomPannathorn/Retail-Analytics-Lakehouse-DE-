from __future__ import annotations

import pandas as pd

from src.common.config import PROCESSED_DIR
from src.common.io import read_dataframe


def assert_no_nulls(df: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        if df[column].isna().any():
            raise ValueError(f"Null values found in required column: {column}")


def assert_unique(df: pd.DataFrame, columns: list[str]) -> None:
    if df.duplicated(columns).any():
        raise ValueError(f"Duplicate records found for key: {columns}")


def assert_non_negative(df: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        if (df[column] < 0).any():
            raise ValueError(f"Negative values found in column: {column}")


def run_checks() -> None:
    order_facts = read_dataframe(PROCESSED_DIR / "silver_order_facts.csv")
    customer_summary = read_dataframe(PROCESSED_DIR / "silver_customer_summary.csv")

    assert_no_nulls(order_facts, ["order_id", "customer_id", "product_id", "net_amount", "payment_status"])
    assert_unique(order_facts, ["order_id"])
    assert_non_negative(order_facts, ["gross_amount", "net_amount", "discount_amount", "delivery_days"])
    assert_no_nulls(customer_summary, ["customer_id", "total_revenue"])

    print("Quality checks passed.")


if __name__ == "__main__":
    run_checks()

