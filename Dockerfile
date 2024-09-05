# Utilizo un FROM de python
FROM python:3.11

# Establezco el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copio los archivos de la aplicación al directorio de trabajo
COPY ./dags /app/dags/
COPY requirements.txt /app/requirements.txt

# Instalo dependencias, en requirements.txt esta Airflow
RUN pip install -r requirements.txt
