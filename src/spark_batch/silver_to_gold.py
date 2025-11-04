"""Daily job to load Silver data into Postgres warehouse.

This script reads Silver parquet (small demo) and upserts into a star schema in Postgres.
It creates dimensions (dim_date, dim_card, dim_merchant) and facts (fact_transactions,
fact_settlement_daily, fact_fraud_signals) and uses INSERT ... ON CONFLICT for upserts.
"""
import os
import pandas as pd
from sqlalchemy import create_engine, text

SILVER_PATH = os.environ.get('SILVER_PATH', '/data/silver')
POSTGRES_URI = os.environ.get('POSTGRES_URI', 'postgresql://payments:payments@postgres:5432/payments_dw')


def ensure_schema(conn):
    # create dims
    conn.execute(text('''
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id DATE PRIMARY KEY
    )
    '''))

    conn.execute(text('''
    CREATE TABLE IF NOT EXISTS dim_card (
        card_hash TEXT PRIMARY KEY
    )
    '''))

    conn.execute(text('''
    CREATE TABLE IF NOT EXISTS dim_merchant (
        merchant_id TEXT PRIMARY KEY,
        mcc TEXT
    )
    '''))

    # facts
    conn.execute(text('''
    CREATE TABLE IF NOT EXISTS fact_transactions (
        transaction_id TEXT PRIMARY KEY,
        ts_event TIMESTAMP,
        date_id DATE,
        card_hash TEXT,
        merchant_id TEXT,
        amount NUMERIC,
        currency TEXT,
        auth_result TEXT
    )
    '''))

    conn.execute(text('''
    CREATE TABLE IF NOT EXISTS fact_settlement_daily (
        merchant_id TEXT,
        currency TEXT,
        dt DATE,
        txn_count BIGINT,
        gross_amount NUMERIC,
        fees NUMERIC,
        net_amount NUMERIC,
        PRIMARY KEY (merchant_id, currency, dt)
    )
    '''))

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


def upsert_dim(conn, table, key_col, rows):
    # rows: iterable of dicts
    for r in rows:
        conn.execute(text(f"INSERT INTO {table} ({key_col}) VALUES (:val) ON CONFLICT ({key_col}) DO NOTHING"), {'val': r[key_col]})


def main():
    # debug prints to help diagnose silent failures in container runs
    print(f'POSTGRES_URI={POSTGRES_URI}')
    print(f'SILVER_PATH={SILVER_PATH} exists={os.path.exists(SILVER_PATH)}')
    try:
        print('listing silver dir:')
        for p in os.listdir(SILVER_PATH):
            print(' -', p)
    except Exception as _e:
        print('could not list silver dir:', _e)

    try:
        engine = create_engine(POSTGRES_URI)
    except Exception as e:
        print('create_engine failed:', e)
        raise

    # read silver parquet
    if not os.path.exists(SILVER_PATH):
        print(f"Silver path {SILVER_PATH} does not exist. Exiting.")
        return

    df = pd.read_parquet(SILVER_PATH)
    if df.empty:
        print('No rows in Silver; nothing to do.')
        return

    # ensure required columns
    if 'transaction_id' not in df.columns:
        print('No transaction_id found; exiting')
        return

    # normalize date
    df['ts_event'] = pd.to_datetime(df['ts_event'])
    df['date_id'] = df['ts_event'].dt.date

    # Use a staging table and set-based SQL to perform bulk upserts for better performance
    stg_table = 'stg_fact_transactions'

    # Prepare dims values
    # ensure mcc column exists (may be absent in small demo datasets)
    if 'mcc' not in df.columns:
        df['mcc'] = None
    merchants = df[['merchant_id', 'mcc']].dropna(subset=['merchant_id']).drop_duplicates()
    cards = df[['card_hash']].dropna().drop_duplicates()
    dates = df[['date_id']].drop_duplicates()

    # write staging via pandas to_sql (replace) and then perform set-based upserts
    # Note: to_sql requires SQLAlchemy engine
    df_tx = df.drop_duplicates(subset=['transaction_id']).copy()
    tx_cols = ['transaction_id', 'ts_event', 'date_id', 'card_hash', 'merchant_id', 'amount', 'currency', 'auth_result', 'location']
    for c in tx_cols:
        if c not in df_tx.columns:
            df_tx[c] = None

    # Use a connection for transactional operations
    try:
        # Use a single transactional connection for the full load so the connection
        # remains open for all operations (previous code closed the connection
        # immediately after ensure_schema which made subsequent conn.execute calls
        # fail with a closed connection).
        with engine.begin() as conn:
            ensure_schema(conn)

            # bulk upsert dims using set operations
            if not merchants.empty:
                # insert merchants using executemany
                conn.execute(text('''
                INSERT INTO dim_merchant(merchant_id,mcc)
                VALUES (:merchant_id,:mcc)
                ON CONFLICT (merchant_id) DO UPDATE SET mcc=EXCLUDED.mcc
                '''), [dict(merchant_id=r['merchant_id'], mcc=r.get('mcc')) for _, r in merchants.iterrows()])

            if not cards.empty:
                conn.execute(text('''
                INSERT INTO dim_card(card_hash)
                VALUES (:card_hash)
                ON CONFLICT (card_hash) DO NOTHING
                '''), [dict(card_hash=r['card_hash']) for _, r in cards.iterrows()])

            if not dates.empty:
                conn.execute(text('''
                INSERT INTO dim_date(date_id)
                VALUES (:date_id)
                ON CONFLICT (date_id) DO NOTHING
                '''), [dict(date_id=r['date_id']) for _, r in dates.iterrows()])

            # write staging table: drop if exists and create
            conn.execute(text(f'DROP TABLE IF EXISTS {stg_table}'))
            conn.execute(text(f'''
            CREATE TABLE {stg_table} (
                transaction_id TEXT,
                ts_event TIMESTAMP,
                date_id DATE,
                card_hash TEXT,
                merchant_id TEXT,
                amount NUMERIC,
                currency TEXT,
                auth_result TEXT,
                location TEXT
            )
            '''))

            # use pandas to_sql to bulk load into staging
            # pandas will open its own connection; use the same engine URL
            df_tx.to_sql(stg_table, con=engine, if_exists='append', index=False)

            # upsert transactions from staging
            conn.execute(text(f'''
            INSERT INTO fact_transactions(transaction_id, ts_event, date_id, card_hash, merchant_id, amount, currency, auth_result)
            SELECT transaction_id, ts_event, date_id, card_hash, merchant_id, amount, currency, auth_result
            FROM {stg_table}
            ON CONFLICT (transaction_id) DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                date_id = EXCLUDED.date_id,
                card_hash = EXCLUDED.card_hash,
                merchant_id = EXCLUDED.merchant_id,
                amount = EXCLUDED.amount,
                currency = EXCLUDED.currency,
                auth_result = EXCLUDED.auth_result
            '''))

            # insert any fraud signals present in the staging table
            conn.execute(text(f'''
            INSERT INTO fact_fraud_signals(transaction_id, ts_event, card_hash, merchant_id, amount, currency, auth_result, location, reason)
            SELECT transaction_id, ts_event, card_hash, merchant_id, amount, currency, auth_result, location, NULL as reason
            FROM {stg_table}
            WHERE (location IS NOT NULL) OR (auth_result = 'DECLINED')
            ON CONFLICT (transaction_id) DO NOTHING
            '''))

            # rebuild settlement_daily from staging aggregated data
            conn.execute(text(f'''
            INSERT INTO fact_settlement_daily(merchant_id, currency, dt, txn_count, gross_amount, fees, net_amount)
            SELECT merchant_id, currency, date_id as dt, COUNT(*) as txn_count, SUM(amount) as gross_amount,
                   SUM(amount) * 0.02 as fees, SUM(amount) - SUM(amount) * 0.02 as net_amount
            FROM {stg_table}
            GROUP BY merchant_id, currency, date_id
            ON CONFLICT (merchant_id, currency, dt) DO UPDATE SET
                txn_count = EXCLUDED.txn_count,
                gross_amount = EXCLUDED.gross_amount,
                fees = EXCLUDED.fees,
                net_amount = EXCLUDED.net_amount
            '''))

            # drop staging table
            conn.execute(text(f'DROP TABLE IF EXISTS {stg_table}'))

            print('Silver -> Gold load complete (bulk upsert).')
    except Exception as e:
        import traceback
        print('Silver->Gold failed with exception:')
        traceback.print_exc()
        raise


if __name__ == '__main__':
    main()
