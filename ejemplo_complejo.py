from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner' : 'medium',
    'start_date' : days_ago(0)
}
dag = DAG(
    dag_id = 'enlaces',
    default_args=default_args,
    description='Ejemplo simple de varios enlaces',
    schedule_interval='@once'
)

def tarea_uno(**kwargs):
    print("Uno!!")

def tarea_dos(**kwargs):
    print("Dos!!")

def tarea_tres(**kwargs):
    print("Tres!!")

def tarea_cuatro(**kwargs):
    print("Cuatro!!")

def tarea_cinco(**kwargs):
    print("Cinco!!")

def tarea_seis(**kwargs):
    print("Seis!!")

def tarea_siete(**kwargs):
    print("Siete!!")

def tarea_ocho(**kwargs):
    print("Ocho!!")

def tarea_nueve(**kwargs):
    print("Nueve!!")

def tarea_diez(**kwargs):
    print("Diez!!")

tarea_uno = PythonOperator(task_id='tarea_uno',
python_callable=tarea_uno,
dag=dag)

tarea_dos = PythonOperator(task_id='tarea_dos',
python_callable=tarea_dos,
dag=dag)

tarea_tres = PythonOperator(task_id='tarea_tres',
python_callable=tarea_tres,
dag=dag)

tarea_cuatro = PythonOperator(task_id='tarea_cuatro',
python_callable=tarea_cuatro,
dag=dag)

tarea_cinco = PythonOperator(task_id='tarea_cinco',
python_callable=tarea_cinco,
dag=dag)

tarea_seis = PythonOperator(task_id='tarea_seis',
python_callable=tarea_seis,
dag=dag)

tarea_siete = PythonOperator(task_id='tarea_siete',
python_callable=tarea_siete,
dag=dag)

tarea_ocho = PythonOperator(task_id='tarea_ocho',
python_callable=tarea_ocho,
dag=dag)

tarea_nueve = PythonOperator(task_id='tarea_nueve',
python_callable=tarea_nueve,
dag=dag)

tarea_diez = PythonOperator(task_id='tarea_diez',
python_callable=tarea_diez,
dag=dag)

tarea_uno >> tarea_dos
tarea_dos >> tarea_tres

tarea_tres >> tarea_cuatro
tarea_tres >> tarea_cinco
tarea_tres >> tarea_seis

tarea_cuatro >> tarea_siete
tarea_cinco >> tarea_siete

tarea_seis >> tarea_ocho

tarea_siete >> tarea_nueve
tarea_ocho >> tarea_nueve

tarea_nueve >> tarea_diez