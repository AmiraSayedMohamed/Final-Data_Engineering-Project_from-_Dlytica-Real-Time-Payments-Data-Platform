# Architecture

High-level flow:

- Kafka (payments.raw, payments.deadletter) → Spark Structured Streaming
- Streaming job writes Bronze Parquet files (partitioned by date) and records fraud signals to Postgres
- Airflow orchestrates Bronze→Silver hourly and Silver→Gold daily
- Silver Parquet is processed by a Spark batch job into Postgres (star schema)
- dbt runs models and tests on Postgres and generates docs
- Reports (CSV) are exported from Postgres daily

Diagram (ASCII):

Kafka -> Spark Streaming -> /data/bronze -> Bronze→Silver (Spark) -> /data/silver -> Silver→Gold (Spark) -> Postgres -> dbt -> Reports
