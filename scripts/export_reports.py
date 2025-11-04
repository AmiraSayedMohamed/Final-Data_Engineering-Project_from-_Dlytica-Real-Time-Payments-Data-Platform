"""Export reports from Postgres to CSV files in outputs/ folder.

Usage: set env var POSTGRES_URI or edit default inside script.
"""
import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import date

POSTGRES_URI = os.environ.get('POSTGRES_URI', 'postgresql://payments:payments@localhost:5432/payments_dw')
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'outputs')

def export_settlement_report(conn):
    sql = '''
    select merchant_id, currency, count(*) as txn_count, sum(amount) as total_amount
    from fact_transactions
    group by merchant_id, currency
    '''
    df = pd.read_sql(sql, conn)
    fname = os.path.join(OUT_DIR, f'settlement_{date.today().isoformat()}.csv')
    df.to_csv(fname, index=False)
    print('Wrote', fname)

def export_fraud_report(conn):
    # Example: if fraud signals table exists
    sql = '''
    select * from fact_fraud_signals
    '''
    try:
        df = pd.read_sql(sql, conn)
    except Exception:
        print('No fraud table found; skipping')
        return
    fname = os.path.join(OUT_DIR, f'fraud_{date.today().isoformat()}.csv')
    df.to_csv(fname, index=False)
    print('Wrote', fname)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    engine = create_engine(POSTGRES_URI)
    with engine.connect() as conn:
        export_settlement_report(conn)
        export_fraud_report(conn)

if __name__ == '__main__':
    main()
