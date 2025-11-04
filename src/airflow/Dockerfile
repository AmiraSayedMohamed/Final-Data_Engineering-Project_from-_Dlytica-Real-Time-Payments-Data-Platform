FROM apache/airflow:2.7.1

# Install dbt and psycopg2 as the 'airflow' user per Airflow Dockerfile guidance
USER root
RUN apt-get update \
	&& apt-get install -y --no-install-recommends build-essential wget \
	&& rm -rf /var/lib/apt/lists/*

# Run pip as the 'airflow' user to avoid permission warnings/failures
RUN su -s /bin/bash airflow -c "pip install --no-cache-dir dbt-core dbt-postgres psycopg2-binary"

# switch back to airflow user
USER airflow

WORKDIR /opt/airflow

# ensure dags and src are mounted from host via docker-compose
