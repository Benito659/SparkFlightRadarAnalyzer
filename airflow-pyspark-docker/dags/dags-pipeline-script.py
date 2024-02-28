from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'extraction_transformation_dag_id',
    default_args=default_args,
    description='DAG to run two Python scripts in order',
    schedule_interval=timedelta(heures=2),
)

def run_call_extraction_flight_script():
    command = ['python', '/opt/airflow/pythonscripts/call-script-extraction-flight.py']
    subprocess.run(command, check=True)
def run_call_transformation_flight_script():
    command = ['python', '/opt/airflow/pythonscripts/call-script-transformation-flight.py']
    subprocess.run(command, check=True)
start_task = DummyOperator(task_id='start', dag=dag)
flight_radar_test_task = PythonOperator(
    task_id='run_call_extraction_flight_script', python_callable=run_call_extraction_flight_script,
    dag=dag, timeout=3600
)
transformation_phase_task = PythonOperator(
    task_id='run_call_transformation_flight_script', python_callable=run_call_transformation_flight_script,
    dag=dag, timeout=3600
)
end_task = DummyOperator(task_id='end', dag=dag)
start_task >> flight_radar_test_task >> transformation_phase_task >> end_task