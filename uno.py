from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

# https://towardsdatascience.com/dependencies-between-dags-in-apache-airflow-2f5935cde3f0

dag = DAG(
    dag_id = 'dependencia_uno',
    schedule_interval='@once',
    #owner: 'test',
    start_date=days_ago(0),
    catchup=False
)

def print_success_message(**kwargs):
    print("Success!!")

def print_end_message(**kwargs):
    print("END")


delay_one_min: BashOperator = BashOperator(task_id="delay_one_min",
dag=dag,
bash_command="sleep 1m")

success = PythonOperator(task_id='success',
python_callable=print_success_message,
dag=dag)

end = PythonOperator(task_id='end',
python_callable=print_end_message,
dag=dag)

success >> delay_one_min >> end