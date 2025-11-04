# Runbook

Start the stack (PowerShell):

```powershell
docker compose up -d
```

Run streaming producer:

```powershell
docker compose run --rm producer python /app/generate_payments.py
```

Run Bronze→Silver manually (for testing):

```powershell
docker compose run --rm spark python /app/spark_batch/bronze_to_silver.py
```

Run Silver→Gold manually (note: may be slow for large datasets):

```powershell
docker compose run --rm spark python /app/spark_batch/silver_to_gold.py
```

Generate reports (writes to ./data/reports):

```powershell
docker compose run --rm spark python /app/src/reports/generate_reports.py
```

Backfill guidance:
- To reprocess Bronze files, move or copy Bronze parquet files into `/data/bronze` and run the Bronze→Silver job with an explicit date partition.

Recovery:
- If Airflow or Spark fails, inspect logs under `./logs` (Airflow) or container logs via `docker compose logs <service>`.
# Runbook — Real-Time Payments Data Platform

This runbook explains how to start the platform, create Kafka topics, run the producer, and execute batch/streaming jobs.

## Services started via Docker Compose

A `docker-compose.yml` is included to start basic services (Zookeeper, Kafka, Postgres, Airflow).

From PowerShell (run as admin if needed):

```powershell
cd "D:\projects\Dlytica Final Project"
docker compose up -d
```

Wait until the containers are healthy.

## Create Kafka topics

Use the helper script:

```powershell
# from project root
bash infra/kafka/create_topics.sh kafka
```

Or use docker exec to run kafka-topics.sh inside the Kafka container.

## Run producer (locally)

Install dependencies and run:

```powershell
cd src/producer
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python generate_payments.py --rate 10 --total 0
```

## Run Spark streaming job (recommended via spark-submit in container)

Example (if you have Spark locally):

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 src/spark_stream/stream_payments.py
```

## Run Bronze→Silver (hourly) and Silver→Gold (daily)

Airflow DAGs are in `dags/`. You can trigger them via the Airflow UI (http://localhost:8080).

## Backfill

Run batch jobs with additional parameters specifying date ranges (not yet implemented in this skeleton).

## Recovery

- If streaming job fails, Spark checkpoint directory is used to resume.
- To replay data, adjust Kafka offsets and restart streaming job.

## Fraud rules

- High Amount: amount &gt; 10,000
- Velocity: &gt;5 txns per card in 1 minute
- Cross-border: same card used in different countries within 10 minutes
- Blacklisted merchant: merchant_id in blacklist
- Decline Rate: &gt;50% declines in last 10 attempts

***
This runbook is a starter; follow README.md for more details.
