from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from etl import (
    get_data_api,
    conexion_db,
    carga_datos_db)


# Defino los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 9, 1),
}

# Defino el DAG
with DAG('consulta_api_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    # Tarea para conectarse a la API y extraer datos
        get_data_api_task = PythonOperator(
            task_id='get_data_api',
            python_callable=get_data_api,
            provide_context=True,
        )
    # Tarea para transformar los datos
#        transformacion_task = PythonOperator(
#            task_id='transformacion',
#            python_callable=transformacion,
#            provide_context=True,
#        )
           
    # Tarea para conectarse a la Base de Datos
        conexion_db_task = PythonOperator(
            task_id='conexion_db',
            python_callable=conexion_db,
        ) 
        
    # Tarea para la carga de datos
        carga_datos_db_task = PythonOperator(
            task_id='carga_datos_db',
            python_callable=carga_datos_db,
        )            

# Defino las dependencias entre tareas
get_data_api_task >> conexion_db_task >> carga_datos_db_task        