###################################################################################

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import logging
import boto3
from botocore.exceptions import ClientError

###################################################################################

default_args = {
    'owner' : 'boto3',
    'start_date' : days_ago(0)
}

dag = DAG(
    dag_id = 'create_bucket',
    default_args=default_args,
    description='Ejemplo simple de como crear un bucket usando boto3, simulado en localstack',
    schedule_interval='@once'
)

###################################################################################

session = boto3.session.Session()

s3_client = session.client(
    service_name='s3',
    endpoint_url='http://192.168.49.2:31566',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
)

###################################################################################

def inicio(**kwargs):
    print("Iniciando proceso...")

def bucket(bucket_name='raw-structured', s3_client=s3_client, region='us-west-2'):
    try:
        location = {'LocationConstraint': region}
        s3_client.create_bucket(Bucket=bucket_name,
                                CreateBucketConfiguration=location)
        print('Bucket creado exitosamente')
    except:
        print('El bucket ya existe')

###################################################################################

create_bucket = PythonOperator(task_id='create_bucket',
                            python_callable=bucket,
                            dag=dag)

inicio = PythonOperator(task_id='inicio',
                        python_callable=inicio,
                        dag=dag)

###################################################################################

inicio >> create_bucket

###################################################################################