"""PySpark Structured Streaming job (clean, single-version).

This file is a clean, single copy of the streaming job. It reads JSON
messages from Kafka, validates them, computes simple fraud signals,
writes valid events to Bronze Parquet, and persists fraud signals to Postgres.

Designed for demo-scale micro-batches and clarity.
"""

import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any

from pyspark.sql import SparkSession

from kafka import KafkaProducer
from sqlalchemy import create_engine, text

from fraud import high_amount, decline_rate


BROKER = os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092')
INPUT_TOPIC = os.environ.get('INPUT_TOPIC', 'payments.raw')
DEADLETTER_TOPIC = os.environ.get('DEADLETTER_TOPIC', 'payments.deadletter')
BRONZE_PATH = os.environ.get('BRONZE_PATH', '/data/bronze')
CHECKPOINT_PATH = os.environ.get('CHECKPOINT_PATH', '/data/checkpoints/stream_payments')
POSTGRES_URI = os.environ.get('POSTGRES_URI', 'postgresql://payments:payments@postgres:5432/payments_dw')


def country_from_location(loc: str):
    if not loc:
        return None
    parts = [p.strip() for p in loc.split(',')]
    return parts[-1] if parts else None


def send_to_deadletter(producer: KafkaProducer, rows: List[Dict[str, Any]]):
    for r in rows:
        # Convert any datetimes to ISO strings so JSON serialization succeeds
        sanitized = {}
        for k, v in r.items():
            if isinstance(v, datetime):
                sanitized[k] = v.isoformat()
            else:
                sanitized[k] = v
        producer.send(DEADLETTER_TOPIC, value=json.dumps(sanitized).encode('utf-8'))
    producer.flush()


def ensure_tables(engine):
    with engine.begin() as conn:
        conn.execute(text('''
        CREATE TABLE IF NOT EXISTS fact_fraud_signals (
            transaction_id TEXT PRIMARY KEY,
            ts_event TIMESTAMP,
            card_hash TEXT,
            merchant_id TEXT,
            amount NUMERIC,
            currency TEXT,
            auth_result TEXT,
            location TEXT,
            reason TEXT,
            created_at TIMESTAMP DEFAULT now()
        )
        '''))

        conn.execute(text('''
        CREATE TABLE IF NOT EXISTS card_state (
            card_hash TEXT PRIMARY KEY,
            last_events JSONB,
            updated_at TIMESTAMP
        )
        '''))


def process_batch(df, epoch_id):
    """Process a Spark micro-batch using pandas for small-batch aggregation.

    - df: Spark DataFrame with kafka 'value' column (bytes)
    - epoch_id: micro-batch id
    """
    if df.rdd.isEmpty():
        return

    pdf = df.selectExpr("CAST(value AS STRING) as json_str").toPandas()
    records = pdf['json_str'].apply(json.loads).tolist()

    producer = KafkaProducer(bootstrap_servers=[BROKER])
    engine = create_engine(POSTGRES_URI)
    ensure_tables(engine)

    valid_rows: List[Dict[str, Any]] = []
    dead_rows: List[Dict[str, Any]] = []
    fraud_rows: List[Dict[str, Any]] = []

    # group events by card_hash
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in records:
        txn = r.get('transaction_id')
        amount = r.get('amount')
        ts = r.get('ts_event')
        try:
            ts_dt = datetime.fromisoformat(ts.replace('Z', '+00:00')) if ts else None
        except Exception:
            ts_dt = None
        card = r.get('card_hash')

        if not txn or amount is None:
            r['reason'] = 'missing_tx_or_amount'
            dead_rows.append(r)
            continue

        entry = {
            'transaction_id': txn,
            'ts_event': ts if ts else (ts_dt.isoformat() if ts_dt else None),
            'ts_dt': ts_dt,
            'card_hash': card,
            'merchant_id': r.get('merchant_id'),
            'amount': amount,
            'currency': r.get('currency'),
            'auth_result': r.get('auth_result'),
            'location': r.get('location'),
            'country': country_from_location(r.get('location')),
        }
        groups.setdefault(card, []).append(entry)

    # load previous state (best-effort)
    with engine.begin() as conn:
        try:
            rows = conn.execute(text('select card_hash, last_events from card_state')).fetchall()
            prev_state = {r[0]: r[1] for r in rows}
        except Exception:
            prev_state = {}

    # process groups
    for card, events in groups.items():
        events = sorted(events, key=lambda x: x['ts_dt'] or datetime.min)

        # velocity (>5 in 1 minute window within this batch)
        times = [e['ts_dt'] for e in events if e['ts_dt']]
        vel = False
        if times:
            for i in range(len(times)):
                cnt = sum(1 for t in times if (t - times[i]) <= timedelta(minutes=1))
                if cnt > 5:
                    vel = True
                    break

        # cross-border within 10 minutes
        countries = [(e['country'], e['ts_dt']) for e in events if e['country'] and e['ts_dt']]
        cross = False
        for i in range(len(countries)):
            for j in range(i + 1, len(countries)):
                if countries[i][0] != countries[j][0] and abs((countries[j][1] - countries[i][1]).total_seconds()) <= 600:
                    cross = True
                    break
            if cross:
                break

        prev = prev_state.get(card) or []
        prev_auths = [p.get('auth_result') for p in prev] if prev else []
        current_auths = [e['auth_result'] for e in events if e['auth_result']]
        decline = decline_rate((prev_auths + current_auths)[-10:], threshold=0.5)

        for e in events:
            reasons: List[str] = []
            if high_amount(e['amount']):
                reasons.append('high_amount')
            if vel:
                reasons.append('velocity')
            if cross:
                reasons.append('cross_border')
            if decline:
                reasons.append('decline_rate')

            if reasons:
                e['reason'] = ','.join(reasons)
                fraud_rows.append(e)
            else:
                valid_rows.append(e)

        # persist card_state (last 10)
        new_state = []
        for e in (prev or []) + events:
            ts_val = e.get('ts_event') or e.get('ts')
            ts_iso = ts_val.isoformat() if isinstance(ts_val, datetime) else ts_val
            new_state.append({'ts': ts_iso, 'auth_result': e.get('auth_result'), 'country': e.get('country')})
        new_state = new_state[-10:]
        with engine.begin() as conn:
            conn.execute(text('''
            INSERT INTO card_state(card_hash, last_events, updated_at)
            VALUES (:card_hash, CAST(:last_events AS jsonb), now())
            ON CONFLICT (card_hash) DO UPDATE SET last_events = CAST(:last_events AS jsonb), updated_at = now()
            '''), {'card_hash': card, 'last_events': json.dumps(new_state)})

    # write Bronze parquet for valid_rows
    if valid_rows:
        import pandas as pd
        vdf = pd.DataFrame(valid_rows)
        vdf['ts_event'] = pd.to_datetime(vdf['ts_event'])
        vdf['date'] = vdf['ts_event'].dt.date
        os.makedirs(BRONZE_PATH, exist_ok=True)
        fname = os.path.join(BRONZE_PATH, f'batch_{epoch_id}.parquet')
        vdf.to_parquet(fname, index=False)

    if dead_rows:
        send_to_deadletter(producer, dead_rows)

    if fraud_rows:
        send_to_deadletter(producer, fraud_rows)
        import pandas as pd
        fdf = pd.DataFrame(fraud_rows)
        fdf['ts_event'] = pd.to_datetime(fdf['ts_event'])
        with engine.begin() as conn:
            for _, row in fdf.iterrows():
                conn.execute(text('''
                insert into fact_fraud_signals(transaction_id, ts_event, card_hash, merchant_id, amount, currency, auth_result, location, reason)
                values(:transaction_id, :ts_event, :card_hash, :merchant_id, :amount, :currency, :auth_result, :location, :reason)
                on conflict (transaction_id) do nothing
                '''), {
                    'transaction_id': row['transaction_id'],
                    'ts_event': row['ts_event'],
                    'card_hash': row['card_hash'],
                    'merchant_id': row['merchant_id'],
                    'amount': row['amount'],
                    'currency': row.get('currency'),
                    'auth_result': row.get('auth_result'),
                    'location': row.get('location'),
                    'reason': row.get('reason')
                })

    producer.close()


def main():
    spark = SparkSession.builder.appName('payments_stream').getOrCreate()

    df = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', BROKER) \
        .option('subscribe', INPUT_TOPIC) \
        .option('startingOffsets', 'earliest') \
        .load()

    df.writeStream.format('console').option('truncate', False).foreachBatch(process_batch).option('checkpointLocation', CHECKPOINT_PATH).start()

    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
