from curses import raw
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append("/opt/airflow/code")
from scraper.ingest_data_raw import main as ingest_raw_data
from etl.bronze import main as bronze_transform 
from etl.silver import main as silver_transform
from etl.gold import main as gold_transform
from vectordb.vector_db import main as load_vectordb


default_args = {
    "owner": "Jhabindra Aryal",
    "start_date": datetime(2025, 7, 20),
    "retries": 1,
}

with DAG(
    "ETL_Pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="ETL pipeline for home assignment",
    tags=["etl"],
) as dag:
    
    raw_task = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=ingest_raw_data,
    )

    bronze_task = PythonOperator(
        task_id="bronze_transform",
        python_callable=bronze_transform,
    )

    silver_task = PythonOperator(
        task_id="silver_transform",
        python_callable=silver_transform,
    )

    gold_task = PythonOperator(
        task_id="gold_transform",
        python_callable=gold_transform,
    )

    vectordb_task = PythonOperator(
        task_id="load_vectordb",
        python_callable=load_vectordb,
    )

    raw_task >> bronze_task >> silver_task >> gold_task >> vectordb_task