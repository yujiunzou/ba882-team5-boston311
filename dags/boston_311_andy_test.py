from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def check_pipeline():
    print("Boston 311 pipeline is ready!")
    print("Andy's first Airflow DAG is working!")

with DAG(
    dag_id="boston_311_andy_test",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["boston311"],
) as dag:

    test_task = PythonOperator(
        task_id="check_pipeline",
        python_callable=check_pipeline,
    )

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def check_pipeline():
    print("Boston 311 pipeline is ready!")
    print("Andy's first Airflow DAG is working!")

with DAG(
    dag_id="boston_311_andy_test",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["boston311"],
) as dag:

    test_task = PythonOperator(
        task_id="check_pipeline",
        python_callable=check_pipeline,
    )
