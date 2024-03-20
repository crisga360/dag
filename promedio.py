from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

# Definir la función para seleccionar los mejores promedios
def seleccionar_mejores_promedios():
    # Cargar el archivo CSV de estudiantes
    df = pd.read_csv('/home/airflow/dag/estudiantes.csv')

    # Ordenar los estudiantes por promedio en orden descendente
    df = df.sort_values(by='promedio', ascending=False)

    # Seleccionar los 5 mejores estudiantes
    mejores_estudiantes = df.head(5)

    # Imprimir los mejores estudiantes
    print("Los mejores estudiantes son:")
    print(mejores_estudiantes)

# Definir los argumentos del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'seleccionar_mejores_promedios',
    default_args=default_args,
    description='Seleccionar los mejores estudiantes por promedio',
    schedule_interval=None,
)

# Definir el operador Python
seleccionar_mejores_promedios_operator = PythonOperator(
    task_id='seleccionar_mejores_promedios_task',
    python_callable=seleccionar_mejores_promedios,
    dag=dag,
)

# Establecer las dependencias entre tareas
seleccionar_mejores_promedios_operator

if __name__ == "__main__":
    dag.cli()
