#Conexión a la API y extracción de datos
import requests
import json
import pandas as pd
import psycopg2

# Función para interactuar con la API
def get_data_api(**kwargs):
    url = "https://api.argentinadatos.com/v1/cotizaciones/"
    
    # Realizar la solicitud a la API
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
    
    # Crear la columna autonumber con cuatro dígitos
    df['autonumber'] = range(1, len(df) + 1)
    df['autonumber'] = df['autonumber'].apply(lambda x: str(x).zfill(4))

    # Concatenar el autonumber con el campo moneda
    df['id_cotizacion'] = df['autonumber']
    #+ '-' + df['moneda']

    # Eliminar la columna autonumber si no la necesitas
    df.drop('autonumber', axis=1, inplace=True)

    # Reordenar las columnas para que 'ID' sea la primera
    df = df[['id_cotizacion', 'moneda', 'compra', 'venta', 'fecha', 'casa']]
    print("DataFrame ordenado")
    print(df)

    # Reemplazar los valores NaN por 'No aplica'
    df['casa'] = df['casa'].fillna('No aplica')

    # Contar los valores nulos por columna
    print("\nConteo de valores nulos por columna:")
    print(df.isnull().sum())

    # Detectar filas duplicadas
    duplicados = df.duplicated()
    print("Duplicados en el DataFrame:")
    print(duplicados)

    # Contar filas duplicadas
    num_duplicados = df.duplicated().sum()
    print(f"Número de filas duplicadas: {num_duplicados}")
    
def conexion_db(**kwargs):
    # Conexión a Redshift
    from dotenv import load_dotenv
    import os

    load_dotenv()

    USUARIO = os.getenv("USUARIO")
    PASSWORD = os.getenv("PASSWORD")
    HOST = os.getenv("HOST")
    PORT = os.getenv("PORT")
    DATABASE = os.getenv("DATABASE")

    try:
        conn = psycopg2.connect(
            dbname=DATABASE,
            user=USUARIO,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        
        #Creo un cursor y la tabla
        cur = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS cotizaciones (
            id_cotizacion INTEGER,
            moneda VARCHAR(255),
            compra FLOAT,
            venta FLOAT,
            fecha date,
            casa VARCHAR(255)
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        print("Conectado a Redshift con éxito!")

    except Exception as e:
        print("No es posible conectar a Redshift")
        print(e)
                
# Defino el nombre de la tabla
    cotizaciones = 'cotizaciones'

    try:
        # Iteración sobre las filas del DataFrame y construir el INSERT
        for index, row in df.iterrows():
            # Construyo INSERT
            insert_query = f"""
            INSERT INTO {cotizaciones} (id_cotizacion, moneda, compra, venta, fecha, casa)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            try:
                # Ejecuto la sentencia con los valores
                cur.execute(insert_query, (row['id_cotizacion'], row['moneda'], row['compra'], row['venta'], row['fecha'], row['casa']))
            except Exception as e:
                print(f"Error al insertar la fila {index}: {e}")

        # Confirmo la transacción
        conn.commit()
        print("Datos insertados exitosamente.")

        # Ejecuto una consulta para verificar
        cur.execute("SELECT * FROM ProyectoDE LIMIT 5")
        result = cur.fetchall()
        for row in result:
            print(row)

    except Exception as e:
        print(f"Ocurrió un error: {e}")
    finally:
        # Cierro el cursor y la conexión
        cur.close()
        conn.close()  
        
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def envio_de_email(**context):
    subject = context["var"]["value"].get("subject_mail")
    from_address = context["var"]["value"].get("email")
    password = context["var"]["value"].get("email_password")
    to_address = context["var"]["value"].get("to_address")

    # Creación del objeto MIMEText
    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Subject'] = subject

    # Creación del contenido del cuerpo del mail con HTML
    html_content = f"""
    <html>
    <body>
        <p>Buenos días</p>
        <p>El proceso de ETL a redshift se ha realizado con éxito</p>
    </body>
    </html>
    """

    # Adjuntar el contenido HTML
    msg.attach(MIMEText(html_content, 'html'))

    try:
        
        server = smtplib.SMTP('smtp.gmail.com', 587) 
        server.starttls() 

        server.login(from_address, password)

        text = msg.as_string()
        server.sendmail(from_address, to_address, text)
        server.quit()
        print("Email sent successfully")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")  
