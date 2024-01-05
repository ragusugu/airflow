from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'purchase_details_dag',
    default_args=default_args,
    schedule_interval='*/4 * * * *',  # Runs every 4 minutes
    catchup=False,
)

insert_purchase_details_task = PostgresOperator(
    task_id='insert_purchase_details',
    postgres_conn_id='postgres_connection_id',
    sql="""
    INSERT INTO purchase_details (customer_id, total_orders, total_returns, created_time)
    SELECT o.customer_id, COUNT(o.order_id) AS total_orders, COUNT(pr.return_id) AS total_returns, CURRENT_TIMESTAMP
    FROM orders o
    LEFT JOIN product_returns pr ON o.customer_id = pr.customer_id
    GROUP BY o.customer_id;
    """,
    dag=dag,
)

