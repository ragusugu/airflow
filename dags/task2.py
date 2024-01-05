from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default_args dictionary to specify the default parameters for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance
dag = DAG(
    'my_parallel_operations',
    default_args=default_args,
    description='A simple DAG for parallel operations',
    schedule_interval=None,  # Set to None to manually trigger the DAG
)

# Function to perform addition
def add_values(a, b, **kwargs):
    sum_result = a + b
    print(f"Sum result: {sum_result}")

# Function to perform subtraction
def subtract_values(c, d, **kwargs):
    subtract_result = c - d
    print(f"Subtraction result: {subtract_result}")

# Function to perform multiplication
def multiply_values(e, f, **kwargs):
    multiply_result = e * f
    print(f"Multiplication result: {multiply_result}")

# Define three tasks, one for each operation
add_task = PythonOperator(
    task_id='add_task',
    python_callable=add_values,
    op_args=[5, 10],
    provide_context=True,
    dag=dag,
)

subtract_task = PythonOperator(
    task_id='subtract_task',
    python_callable=subtract_values,
    op_args=[15, 7],
    provide_context=True,
    dag=dag,
)

multiply_task = PythonOperator(
    task_id='multiply_task',
    python_callable=multiply_values,
    op_args=[3, 4],
    provide_context=True,
    dag=dag,
)

# Set the tasks to run in parallel
[add_task, subtract_task, multiply_task]
