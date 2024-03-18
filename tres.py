from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
#from airflow.operators.sensors import ExternalTaskSensor
from airflow.sensors import BaseSensorOperator

# https://towardsdatascience.com/dependencies-between-dags-in-apache-airflow-2f5935cde3f0

dag = DAG(
    dag_id = 'dependencia_tres',
    schedule_interval='@once',
    #owner: 'test',
    start_date=days_ago(0),
    catchup=False
)

def print_success_message(**kwargs):
    print("Success!!")

def print_end_message(**kwargs):
    print("END")

externalsensor1 = ExternalTaskSensor(
        task_id='dependencia_dos_completed_Status',
        external_dag_id='dependencia_dos',
        external_task_id=None, 
        check_existence=True)


success = PythonOperator(task_id='success',
                         python_callable=print_success_message,
                         dag=dag)

end = PythonOperator(task_id='end',
                         python_callable=print_end_message,
                         dag=dag)

[externalsensor1] >> success >> end
