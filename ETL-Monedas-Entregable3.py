#Entregable N°3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests

# Defino los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 9, 1),
}

# Defino el DAG
with DAG('api_interaction_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:



# Defino las dependencias entre tareas
