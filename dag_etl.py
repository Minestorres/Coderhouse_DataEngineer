from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from etl import (
    get_data_api,
    conexion_db,
    envio_de_email
    )


# Defino los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    "email":'mariainestorres89@gmail.com',
    "email_on_failure":True,
    "email_on_success":True,
    "email_on_retry":True,
}

# Defino el DAG
with DAG(
         dag_id='consulta_api_dag',
         start_date=datetime(2024, 9, 1),
         schedule='@daily',
         catchup=False,
         default_args=default_args,
) as dag:
    
    # Tarea para conectarse a la API y extraer datos
        get_data_api_task = PythonOperator(
            task_id='get_data_api',
            python_callable=get_data_api,
            provide_context=True,
        )
           
    # Tarea para conectarse a la Base de Datos y cargarlos
        conexion_db_task = PythonOperator(
            task_id='conexion_db',
            python_callable=conexion_db,
        ) 
   
    # Tarea para envío de mails
        envio_de_email_task = PythonOperator(
            task_id='envio_de_email',
            python_callable=envio_de_email
        )
        
# Defino las dependencias entre tareas
get_data_api_task >> conexion_db_task >> envio_de_email_task       
