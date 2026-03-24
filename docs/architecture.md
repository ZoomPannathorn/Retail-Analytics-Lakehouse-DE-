# Architecture Notes

## Problem statement

An e-commerce company needs a reliable daily batch pipeline to consolidate orders, customers, products, and payments into analytics-ready tables for reporting and decision-making.

## Data model

- `raw` schema stores untouched extracted entities.
- `silver` schema stores standardized and enriched datasets.
- `analytics` schema stores reporting views and dbt marts.

## Pipeline flow

1. `generate_raw_data.py` creates deterministic source datasets.
2. `build_silver.py` joins entities and derives business metrics.
3. `checks.py` validates uniqueness, null thresholds, and non-negative monetary values.
4. `load_to_postgres.py` loads source and transformed tables into PostgreSQL.
5. dbt models create star-schema-like marts for BI and ad hoc analysis.
6. Airflow schedules and monitors the pipeline daily.

## Why this is resume-ready

- It shows orchestration, transformation, warehousing, testing, and documentation in one repository.
- The code is easy for recruiters and interviewers to run locally.
- The data model supports clear storytelling around business impact.

## Suggested demo walkthrough

1. Show the Airflow DAG and explain orchestration dependencies.
2. Open the Python transformation layer and describe silver-table enrichment.
3. Query the `analytics.monthly_revenue` view in PostgreSQL.
4. Run dbt tests and explain how data contracts are enforced.
5. Describe how you would migrate this design to AWS, Azure, or GCP.

