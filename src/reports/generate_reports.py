"""Generate settlement and fraud CSV reports from the Postgres warehouse.

Writes CSVs to /data/reports for consumption or export.
"""
import os
import pandas as pd
from sqlalchemy import create_engine

POSTGRES_URI = os.environ.get('POSTGRES_URI', 'postgresql://payments:payments@postgres:5432/payments_dw')
OUT_DIR = os.environ.get('REPORT_OUT', '/data/reports')


def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def export_settlement(engine):
    sql = '''
    SELECT merchant_id, currency, dt as date, txn_count, gross_amount, fees, net_amount
    FROM fact_settlement_daily
    ORDER BY dt DESC
    '''
    df = pd.read_sql(sql, con=engine)
    out = os.path.join(OUT_DIR, 'settlement_report.csv')
    df.to_csv(out, index=False)
    print('Wrote', out, 'rows=', len(df))


def export_fraud(engine):
    sql = '''
    SELECT transaction_id, ts_event, card_hash, merchant_id, amount, currency, auth_result, location, reason, created_at
    FROM fact_fraud_signals
    ORDER BY created_at DESC
    '''
    df = pd.read_sql(sql, con=engine)
    out = os.path.join(OUT_DIR, 'fraud_report.csv')
    df.to_csv(out, index=False)
    print('Wrote', out, 'rows=', len(df))


def main():
    ensure_out_dir()
    engine = create_engine(POSTGRES_URI)
    try:
        export_settlement(engine)
    except Exception as e:
        print('Failed to export settlement:', e)
    try:
        export_fraud(engine)
    except Exception as e:
        print('Failed to export fraud:', e)


if __name__ == '__main__':
    main()
