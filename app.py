from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import sqlite3

# Define ETL functions
def extract():
    """Extract data from sales_data.csv and save to staging area."""
    df = pd.read_csv('/path/to/sales_data.csv')  # Read raw data
    df.to_csv('/path/to/staging/sales_data.csv', index=False)  # Save to staging area

def transform():
    """Transform the data by calculating total_price and save to transformed area."""
    df = pd.read_csv('/path/to/staging/sales_data.csv')  # Read staged data
    df['total_price'] = df['quantity'] * df['unit_price']  # Add new column
    df.to_csv('/path/to/transformed/sales_data.csv', index=False)  # Save transformed data

def load():
    """Load transformed data into SQLite database."""
    conn = sqlite3.connect('/path/to/database.db')  # Connect to database
    df = pd.read_csv('/path/to/transformed/sales_data.csv')  # Read transformed data
    df.to_sql('sales', conn, if_exists='replace', index=False)  # Load into database
    conn.close()  # Close connection

# Define default arguments for DAG
default_args = {
    "start_date": datetime(2024, 3, 1),
    "catchup": False  # Avoid running past instances if DAG is enabled later
}

# Define Airflow DAG
with DAG("etl_pipeline", default_args=default_args, schedule_interval="@daily") as dag:
    # Define tasks
    task_extract = PythonOperator(task_id="extract_data", python_callable=extract)
    task_transform = PythonOperator(task_id="transform_data", python_callable=transform)
    task_load = PythonOperator(task_id="load_data", python_callable=load)

    # Define execution order
    task_extract >> task_transform >> task_load
