from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from Data_Ingestion import ingest_data
from Data_Transformation import transform
from Data_Loading import load


# dag function that manage all tasks
with DAG(dag_id = 'data_pipeline', 
         start_date=datetime(2023,1,1), 
         schedule_interval = '@daily', 
         catchup = False) as dag:
    
    ingest_task = PythonOperator(
        task_id = 'ingestion',
        python_callable = ingest_data,
    )

    transform_task = PythonOperator(
        task_id = 'transformation',
        python_callable = transform,
    )

    load_task = PythonOperator(
        task_id = 'loading',
        python_callable = load,
    )

# Define Task Dependencies
ingest_task >> transform_task >> load_task