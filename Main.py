#Entregable 1

#Conexión a API y descarga de datos
import requests
import json
import pandas as pd

url = "https://api.argentinadatos.com/v1/cotizaciones/"
response = requests.get(url)

#Verificación de estado de la solicitud. En caso de ser exitosa, se almacena la información en formato JSON 
if response.status_code == 200:
    data = response.json()
    print("Carga exitosa")
else: 
    print(f"Error: {response.status_code}")

#Creación del DataFrame para poder trabajar con Pandas
df = pd.DataFrame(data)
print(df)

# Conexión a Redshift
from dotenv import load_dotenv
import os

load_dotenv()

USUARIO = os.getenv("USUARIO")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DATABASE = os.getenv("DATABASE")

import psycopg2

try:
    conn = psycopg2.connect(
        dbname=DATABASE,
        user=USUARIO,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    print("Conectado a Redshift con éxito!")   
except Exception as e:
    print("No es posible conectar a Redshift")
    print(e)

# Creación de la tabla en DBeaver:
# Adjunté imágenes sobre cómo lo realicé en Github



