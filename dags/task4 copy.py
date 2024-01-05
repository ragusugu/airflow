from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
# Define DAG default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Define DAG with default arguments
dag = DAG(
    'postgres_dag',
    default_args=default_args,
    description='every 2 minutes',
    schedule_interval='*/2 * * * *',  # Run every 2 minute
)
# SQL query to insert rows into the "Orders" table
insert_orders_sql = """
INSERT INTO Orders (order_id, customer_id, order_date)
VALUES (DEFAULT, 3, NOW()),
       (DEFAULT, 4, NOW());
"""
# SQL query to insert rows into the "Product_returns" table
insert_returns_sql = """
INSERT INTO Product_returns (return_id, customer_id, return_date)
VALUES (DEFAULT, 3, NOW()),
       (DEFAULT, 4, NOW());
"""
# Operator to execute the SQL query and insert rows into the "Orders" table
insert_orders_task = PostgresOperator(
    task_id='insert_orders_task',
    postgres_conn_id='postgres_connection_id',
    sql=insert_orders_sql,
    dag=dag,
)
# Operator to execute the SQL query and insert rows into the "Product_returns" table
insert_returns_task = PostgresOperator(
    task_id='insert_returns_task',
    postgres_conn_id='postgres_connection_id',
    sql=insert_returns_sql,
    dag=dag,
)
# Set task dependencies
insert_orders_task >> insert_returns_task