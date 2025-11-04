from sqlalchemy import create_engine, text
import os, traceback

uri = os.environ.get('POSTGRES_URI','postgresql://payments:payments@postgres:5432/payments_dw')
print('Using URI:', uri)
try:
    engine = create_engine(uri)
    with engine.connect() as conn:
        r = conn.execute(text('SELECT 1'))
        print('SELECT 1 ->', r.scalar())
        r = conn.execute(text('SELECT current_database(), current_user'))
        print('DB/User ->', r.fetchone())
except Exception:
    print('Connection failed:')
    traceback.print_exc()
