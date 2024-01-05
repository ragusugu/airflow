from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

# Define the default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'task1',
    default_args=default_args,
    description='An example DAG with PythonOperators',
    schedule_interval=timedelta(days=1),  # Set the desired schedule interval
)

# # Define the function to create an Airflow variable with the given input
# def create_airflow_variable(input_list, **kwargs):
#     for item in input_list:
#         Variable.set(item, "some_value")  # You can set any value as per your requirements
# Define the function to create an Airflow variable with the given input
def create_airflow_variable(input_list, **kwargs):
    for item in input_list:
        existing_value = Variable.get(item, default_var=None)
        if existing_value is not None:
            # Variable already exists, update it with a new value
            Variable.set(item, "new_value")
        else:
            # Variable doesn't exist, create it with the specified value
            Variable.set(item, "some_value")

# Define the function to get the variable and find the length of each word in the list
def get_variable_and_compute_length(input_list, **kwargs):
    word_lengths = {word: len(word) for word in input_list}
    print(word_lengths)

# Define the first PythonOperator task to create the Airflow variable
task_create_variable = PythonOperator(
    task_id='create_airflow_variable',
    python_callable=create_airflow_variable,
    op_args=[["DAG", "variable", "preset"]],
    dag=dag,
)

# Define the second PythonOperator task to get the variable and compute word lengths
task_compute_word_lengths = PythonOperator(
    task_id='compute_word_lengths',
    python_callable=get_variable_and_compute_length,
    op_args=[["DAG", "variable", "preset"]],
    dag=dag,
)

# Set the task dependencies
task_create_variable >> task_compute_word_lengths
