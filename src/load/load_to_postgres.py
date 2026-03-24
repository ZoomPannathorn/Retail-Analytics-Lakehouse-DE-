from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from src.common.config import PROCESSED_DIR, RAW_DIR, settings
from src.common.io import read_dataframe


def infer_postgres_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    return "TEXT"


def load_csvs(engine, schema: str, mapping: dict[str, Path]) -> None:
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

        for table_name, path in mapping.items():
            df = read_dataframe(path)
            qualified_table = f"{schema}.{table_name}"

            connection.execute(text(f"DROP TABLE IF EXISTS {qualified_table} CASCADE"))

            column_definitions = ", ".join(
                f'"{column}" {infer_postgres_type(df[column])}' for column in df.columns
            )
            connection.execute(text(f"CREATE TABLE {qualified_table} ({column_definitions})"))

            if df.empty:
                continue

            records = df.where(pd.notnull(df), None).to_dict(orient="records")
            quoted_columns = ", ".join(f'"{column}"' for column in df.columns)
            value_placeholders = ", ".join(f":{column}" for column in df.columns)
            insert_statement = text(
                f"INSERT INTO {qualified_table} ({quoted_columns}) VALUES ({value_placeholders})"
            )
            connection.execute(insert_statement, records)


def create_analytics_views(engine) -> None:
    statements = [
        """
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS analytics;
        """,
        """
        CREATE OR REPLACE VIEW analytics.monthly_revenue AS
        SELECT
            order_month,
            SUM(net_amount) AS revenue,
            COUNT(DISTINCT order_id) AS orders,
            ROUND(AVG(net_amount)::numeric, 2) AS avg_order_value
        FROM silver.order_facts
        WHERE payment_status = 'paid'
        GROUP BY order_month
        ORDER BY order_month;
        """,
        """
        CREATE OR REPLACE VIEW analytics.category_performance AS
        SELECT
            category,
            SUM(net_amount) AS revenue,
            COUNT(DISTINCT order_id) AS orders,
            ROUND(AVG(net_amount)::numeric, 2) AS avg_order_value
        FROM silver.order_facts
        WHERE payment_status = 'paid'
        GROUP BY category
        ORDER BY revenue DESC;
        """,
    ]
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def run() -> None:
    engine = create_engine(settings.sqlalchemy_url)

    raw_files = {
        "customers": RAW_DIR / "customers.csv",
        "products": RAW_DIR / "products.csv",
        "orders": RAW_DIR / "orders.csv",
        "payments": RAW_DIR / "payments.csv",
    }
    silver_files = {
        "customers": PROCESSED_DIR / "silver_customers.csv",
        "products": PROCESSED_DIR / "silver_products.csv",
        "order_facts": PROCESSED_DIR / "silver_order_facts.csv",
        "customer_summary": PROCESSED_DIR / "silver_customer_summary.csv",
    }

    load_csvs(engine, "raw", raw_files)
    load_csvs(engine, "silver", silver_files)
    create_analytics_views(engine)

    print("Loaded raw and silver tables into PostgreSQL.")


if __name__ == "__main__":
    run()
