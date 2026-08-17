# Snowflake + Airflow orchestration (portfolio sample)
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import snowflake.connector


def get_connection():
    return snowflake.connector.connect(
        user="USER",
        password="***",
        account="ACCT",
        warehouse="WH",
        database="DB",
        schema="PUBLIC",
    )


def run_merge():
    sql = """
    MERGE INTO sales_curated tgt
    USING (
      SELECT
        order_id,
        customer_email,
        amount,
        event_ts,
        event_date,
        source_file,
        loaded_at,
        ROW_NUMBER() OVER (
          PARTITION BY order_id
          ORDER BY loaded_at DESC
        ) AS rn
      FROM stg_sales
      WHERE order_id IS NOT NULL
        AND amount IS NOT NULL
        AND amount >= 0
    ) src
    ON tgt.order_id = src.order_id
    WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
      customer_email = src.customer_email,
      amount = src.amount,
      event_ts = src.event_ts,
      event_date = src.event_date,
      source_file = src.source_file,
      updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND src.rn = 1 THEN INSERT (
      order_id, customer_email, amount, event_ts, event_date,
      source_file, created_at, updated_at
    ) VALUES (
      src.order_id, src.customer_email, src.amount, src.event_ts,
      src.event_date, src.source_file, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    )
    """

    cnx = get_connection()
    cur = cnx.cursor()
    try:
        cur.execute(sql)
    finally:
        cur.close()
        cnx.close()


def validate_load():
    checks = {
        "daily_rows": """
            SELECT COUNT(*)
            FROM sales_curated
            WHERE event_date = CURRENT_DATE()
        """,
        "null_keys": """
            SELECT COUNT(*)
            FROM sales_curated
            WHERE order_id IS NULL
        """,
        "duplicate_keys": """
            SELECT COUNT(*)
            FROM (
              SELECT order_id
              FROM sales_curated
              GROUP BY order_id
              HAVING COUNT(*) > 1
            )
        """,
        "invalid_amounts": """
            SELECT COUNT(*)
            FROM sales_curated
            WHERE amount < 0
        """,
    }

    cnx = get_connection()
    cur = cnx.cursor()
    try:
        results = {}
        for name, sql in checks.items():
            cur.execute(sql)
            results[name] = cur.fetchone()[0]

        assert results["daily_rows"] > 0, "No curated records available for today"
        assert results["null_keys"] == 0, "Null order_id values detected"
        assert results["duplicate_keys"] == 0, "Duplicate order_id values detected"
        assert results["invalid_amounts"] == 0, "Negative sales amounts detected"
    finally:
        cur.close()
        cnx.close()


with DAG(
    dag_id="snowflake_sales_refresh",
    default_args={
        "owner": "ruheena",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "snowflake", "data-quality"],
) as dag:
    transform = PythonOperator(
        task_id="merge_staging_to_curated",
        python_callable=run_merge,
    )

    validate = PythonOperator(
        task_id="validate_curated_sales",
        python_callable=validate_load,
    )

    transform >> validate
