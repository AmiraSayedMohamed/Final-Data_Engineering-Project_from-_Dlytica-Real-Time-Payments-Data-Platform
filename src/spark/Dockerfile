FROM python:3.10-slim
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    build-essential \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark kafka-python pandas sqlalchemy psycopg2-binary pyarrow

WORKDIR /app
COPY ./ /app

CMD ["bash"]
