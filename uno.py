from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

# Definir los argumentos del DAG
default_args = {
    'owner': 'usuario',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
with DAG('sistema_recomendacion_peliculas_spark',
         default_args=default_args,
         schedule_interval=timedelta(days=1)) as dag:

    # Tarea para ejecutar el trabajo Spark para adquirir datos
    acquire_data_task = SparkSubmitOperator(
        task_id='acquire_data',
        conn_id='spark_default',  # Conexión a Spark definida en Airflow
        application='/home/aiflow/dag/acquire_data.py',  # Ruta al script Spark
        verbose=True
    )

    # Tarea para preprocesar datos
    preprocess_data_task = SparkSubmitOperator(
        task_id='preprocess_data',
        conn_id='spark_default',
        application='/home/aiflow/dag/preprocess_data.py',
        verbose=True
    )

    # Tarea para analizar datos y generar recomendaciones
    analyze_data_task = SparkSubmitOperator(
        task_id='analyze_data',
        conn_id='spark_default',
        application='/home/aiflow/dag/analyze_data.py',
        verbose=True
    )

    # Tarea para presentar los resultados
    present_results_task = SparkSubmitOperator(
        task_id='present_results',
        conn_id='spark_default',
        application='/home/aiflow/dag/present_results.py',
        verbose=True
    )

    # Definir dependencias entre tareas
    acquire_data_task >> preprocess_data_task >> analyze_data_task >> present_results_task

