"""Hourly batch job: read Bronze parquet, deduplicate, standardize and write Silver parquet."""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

BRONZE_PATH = os.environ.get('BRONZE_PATH', '/data/bronze')
SILVER_PATH = os.environ.get('SILVER_PATH', '/data/silver')


def main():
    spark = SparkSession.builder.appName('bronze_to_silver').getOrCreate()
    try:
        # Try the fast Spark path first
        df = spark.read.parquet(BRONZE_PATH)

        # deduplicate by transaction_id keeping latest processing_ts
        windowed = df.orderBy(col('processing_ts').desc())
        dedup = windowed.dropDuplicates(['transaction_id'])

        cleaned = dedup.withColumn('amount', col('amount').cast('double')) \
            .withColumn('currency', col('currency')) \
            .withColumn('cleaned_ts', current_timestamp())

        cleaned.write.mode('overwrite').parquet(SILVER_PATH)
        print('Wrote Silver parquet via Spark')
    except Exception as e:
        # Fall back to pandas/pyarrow when Spark cannot read Parquet timestamp nanos
        print('Spark read failed, falling back to pandas/pyarrow:', e)
        import pandas as pd
        import pyarrow.dataset as ds
        import pyarrow as pa
        import pyarrow.parquet as pq
        from datetime import datetime

        dataset = ds.dataset(BRONZE_PATH, format='parquet')
        table = dataset.to_table()
        pdf = table.to_pandas()

        # choose a timestamp column to sort by (prefer processing_ts, then ts_event)
        sort_col = None
        if 'processing_ts' in pdf.columns:
            sort_col = 'processing_ts'
        elif 'ts_event' in pdf.columns:
            sort_col = 'ts_event'

        if sort_col is not None:
            pdf[sort_col] = pd.to_datetime(pdf[sort_col], utc=True, errors='coerce')
            pdf = pdf.sort_values(sort_col, ascending=False)
        else:
            # no timestamp available; keep the existing order
            print('No timestamp column found in Bronze; deduping by first-seen order')

        # deduplicate keeping the first (which will be the latest if we sorted)
        pdf = pdf.drop_duplicates(subset=['transaction_id'], keep='first')

        pdf['amount'] = pd.to_numeric(pdf.get('amount', pd.Series()), errors='coerce')
        pdf['cleaned_ts'] = datetime.utcnow()

        # ensure output directory exists
        os.makedirs(SILVER_PATH, exist_ok=True)

        # attempt to coerce pandas datetime columns to millisecond precision
        try:
            dt_cols = [c for c in pdf.columns if pd.api.types.is_datetime64_any_dtype(pdf[c])]
            for c in dt_cols:
                try:
                    pdf[c] = pd.to_datetime(pdf[c], utc=True, errors='coerce').dt.tz_convert(None).astype('datetime64[ms]')
                except Exception as _e:
                    # if coercion fails (out-of-range ns values), fall back to ISO strings
                    print(f'Warning: coercing datetime column {c} to ISO strings due to: {_e}')
                    pdf[c] = pd.to_datetime(pdf[c], utc=True, errors='coerce').dt.tz_convert(None).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        except Exception:
            # if pandas datetime introspection itself fails, ignore and let pyarrow handle types
            pass

        # write with pyarrow ensuring millisecond timestamps for Spark compatibility
        table_out = pa.Table.from_pandas(pdf)
        pq.write_table(table_out, os.path.join(SILVER_PATH, 'silver.parquet'), coerce_timestamps='ms', use_deprecated_int96_timestamps=False)
        print('Wrote Silver parquet via pyarrow')


if __name__ == '__main__':
    main()
