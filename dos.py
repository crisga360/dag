from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskSensor

dag = DAG(
    dag_id='dependencia_dos',
    schedule_interval='@once',
    start_date=days_ago(0),
    catchup=False
)

def print_success_message(**kwargs):
    print("Success!!")

def print_end_message(**kwargs):
    print("END")

externalsensor1 = ExternalTaskSensor(
    task_id='dependencia_uno_completed_Status',
    external_dag_id='dependencia_uno',
    external_task_id=None,
    check_existence=True,
    mode="poke",  # Specify sensor mode
    poke_interval=60,  # Adjust poke interval as needed
    timeout=600,  # Adjust timeout as needed
    dag=dag
)

success = PythonOperator(
    task_id='success',
    python_callable=print_success_message,
    dag=dag
)

end = PythonOperator(
    task_id='end',
    python_callable=print_end_message,
    dag=dag
)

externalsensor1 >> success >> end
