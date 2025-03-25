from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import sqlite3

def extract():
    df = pd.read_csv('/workspaces/airflow_project/dags/files/sales_data.csv')
    df.to_csv('/workspaces/airflow_project/dags/files/staging_sales_data.csv', index=False)

def transform():
    df = pd.read_csv('/workspaces/airflow_project/dags/files/staging_sales_data.csv')
    df['total_price'] = df['quantity'] * df['unit_price']
    df.to_csv('/workspaces/airflow_project/dags/files/transformed_sales_data.csv', index=False)

def load():
    conn = sqlite3.connect('/workspaces/airflow_project/dags/files/database.db')
    df = pd.read_csv('/workspaces/airflow_project/dags/files/transformed_sales_data.csv')
    df.to_sql('sales', conn, if_exists='replace', index=False)
    conn.close()

default_args = {
    "start_date": datetime(2024, 3, 1),
    "catchup": False
}

with DAG("etl_pipeline", default_args=default_args, schedule_interval="@daily") as dag:
    task_extract = PythonOperator(task_id="extract_data", python_callable=extract)
    task_transform = PythonOperator(task_id="transform_data", python_callable=transform)
    task_load = PythonOperator(task_id="load_data", python_callable=load)

    task_extract >> task_transform >> task_load
