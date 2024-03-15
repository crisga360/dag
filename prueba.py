# simple_etl_dag.py
from datetime import timedelta
from datetime import datetime
import pandas as pd
# The DAG object
from airflow import DAG
# Operators
from airflow.operators.python_operator import PythonOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Airflow',
    'depends_on_past': True,
    'start_date': datetime(2020, 7, 10, 13),
    # 'end_date': datetime(2020, 7, 20),
    'email': ['your_email@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'Extract_Transform_Load',
    default_args=default_args,
    description='Read and transform a csv file',
    schedule_interval='30 * * * *',
)
def read_csv_file(file_name, *args, **kwargs):
    data = pd.read_csv(file_name, sep=';', 
                       encoding='iso-8859-1', 
                       header='infer')
    return data
def filter_columns(**context):
    data = context['task_instance'].\
        xcom_pull(task_ids='Read_csv_file')
    
    data = data[['ESTACION', 'LONGITUD', 'LATITUD']]
    return data
def load_data(output_path, **context):
    data = context['task_instance'].\
        xcom_pull(task_ids='Filter_columns')
    
    data.to_csv(output_path, sep=',', 
                header=True, 
                encoding='utf-8', 
                index=False)
t1 = PythonOperator(
    task_id='Read_csv_file',
    provide_context=True,
    python_callable=read_csv_file,
    op_kwargs={'file_name': './data/ingest/informacion_estaciones_red_calidad_aire.csv'},
    dag=dag
)
t2 = PythonOperator(
    task_id='Filter_columns',
    provide_context=True,
    python_callable=filter_columns,
    dag=dag
)
t3 = PythonOperator(
    task_id='Save_data',
    provide_context=True,
    python_callable=load_data,
    op_kwargs={'output_path': './data/out/data_for_map.csv'},
    dag=dag
)
t1 >> t2 >> t3
