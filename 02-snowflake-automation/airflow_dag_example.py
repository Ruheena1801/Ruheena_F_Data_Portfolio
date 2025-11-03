# Airflow DAG example (sample)
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import snowflake.connector

def validate_load(**context):
    cnx = snowflake.connector.connect(
        user='USER', password='***', account='ACCT', warehouse='WH',
        database='DB', schema='PUBLIC'
    )
    cur = cnx.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_sales WHERE DATE(loaded_at)=CURRENT_DATE()")
    count = cur.fetchone()[0]
    assert count > 0, "No records loaded today"
    cur.close()
    cnx.close()

with DAG(
    'snowflake_raw_sales_refresh',
    default_args={'owner': 'ruheena', 'retries': 1, 'retry_delay': timedelta(minutes=5)},
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    validate = PythonOperator(task_id='validate_load', python_callable=validate_load)
