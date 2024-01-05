from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'postgres_data_pipeline',
    default_args=default_args,
    description='A simple PostgreSQL data pipeline',
    schedule_interval=timedelta(days=1),
)

# Task to create Customers table
create_customers_table = PostgresOperator(
    task_id='create_customers_table',
    postgres_conn_id='postgres_connection_id',
    sql="""
    CREATE TABLE Customers (
        customer_id SERIAL PRIMARY KEY,
        customer_name VARCHAR(255) NOT NULL,
        place VARCHAR(255) NOT NULL
    )
    """,
    dag=dag,
)

# Task to insert data into Customers table
insert_customers_data = PostgresOperator(
    task_id='insert_customers_data',
    postgres_conn_id='postgres_connection_id',
    sql="""
    INSERT INTO Customers (customer_name, place) VALUES
    ('John Doe', 'New York'),
    ('Jane Smith', 'Los Angeles'),
    ('Bob Johnson', 'Chicago'),
    ('Alice Brown', 'San Francisco'),
    ('Charlie Wilson', 'Miami')
    """,
    dag=dag,
)

# Repeat the process for Orders and Product_returns tables

create_orders_table = PostgresOperator(
    task_id='create_orders_table',
    postgres_conn_id='postgres_connection_id',
    sql="""
    -- Create Orders table
CREATE TABLE Orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES Customers(customer_id),
    order_date DATE NOT NULL
);
    """,
    dag=dag,
)

insert_orders_data = PostgresOperator(
    task_id='insert_orders_data',
    postgres_conn_id='postgres_connection_id',
    sql="""
   -- Insert data into Orders table
INSERT INTO Orders (customer_id, order_date) VALUES
(1, '2024-01-01'),
(2, '2024-01-02'),
(3, '2024-01-03'),
(4, '2024-01-04'),
(5, '2024-01-05');

    """,
    dag=dag,
)

create_product_returns_table = PostgresOperator(
    task_id='create_product_returns_table',
    postgres_conn_id='postgres_connection_id',
    sql="""
    -- Create Product_returns table
CREATE TABLE Product_returns (
    return_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES Customers(customer_id),
    return_date DATE NOT NULL
);

    """,
    dag=dag,
)

insert_product_returns_data = PostgresOperator(
    task_id='insert_product_returns_data',
    postgres_conn_id='postgres_connection_id',
    sql="""
    -- Insert data into Product_returns table
INSERT INTO Product_returns (customer_id, return_date) VALUES
(1, '2024-01-10'),
(3, '2024-01-12'),
(5, '2024-01-15'),
(2, '2024-01-18'),
(4, '2024-01-20');

    """,
    dag=dag,
)

# Set the task dependencies
create_customers_table >> insert_customers_data
insert_customers_data >> create_orders_table
create_orders_table >> insert_orders_data
insert_orders_data >> create_product_returns_table
create_product_returns_table >> insert_product_returns_data
