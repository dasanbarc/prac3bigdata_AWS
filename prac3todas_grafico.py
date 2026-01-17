import os
import subprocess
import sys

# INSTALACIÓN DE MATPLOTLIB Y NUMPY
try:
    print("Actualizando dependencias para resolver incompatibilidad binaria...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "numpy==1.23.5"])
    subprocess.check_call([sys.executable, "-m", "pip", "install", "matplotlib"])
    print("Dependencias instaladas correctamente.")
except Exception as e:
    print(f"Error instalando librerías: {e}")


# Importo el resto de librerías
import boto3
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO, StringIO

# --- CONFIGURACIÓN ---
BUCKET_NAME = 'bigdata-weather-user4688629'
CSV_FOLDER_PREFIX = 'processed/TMAX_yearly_avg_all_stations/'
PNG_DEST_KEY = 'visuals/media_tmax_anual_todas_estaciones_meteo.png'
s3_client = boto3.client('s3')

def run_glue_job():
    print("Iniciando Job de Visualización...")

    # 1. ENCONTRAR EL NOMBRE DEL ÚNICO CSV Y LEERLO
    # Listar los objetos dentro de la carpeta
    objetos = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=CSV_FOLDER_PREFIX)

    # Buscar el archivo .csv de interés (el que empieza con "part-")
    csv_key = None
    for obj in objetos.get('Contents', []):
        if obj['Key'].endswith('.csv') and 'part-' in obj['Key']:
            csv_key = obj['Key']
            break

    if not csv_key:
        print("No se encontró el archivo CSV. Revisa si el primer script terminó correctamente.")
        return

    print(f"Leyendo el archivo detectado: {csv_key}")

    # Leer el objeto con la Key exacta que hemos encontrado
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=csv_key)
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))

    # Ordenar por año para que la línea salga bien
    df = df.sort_values('year')

    # 2. GRAFICAR CON MATPLOTLIB
    plt.figure(figsize=(12, 6))
    plt.plot(df['year'], df['avg_temp'], marker='o', color='tab:red', linestyle='-', linewidth=2)

    plt.title('Promedio Anual de Temperaturas Máximas - Todas las estaciones meteorológicas', fontsize=14)
    plt.xlabel('Año', fontsize=12)
    plt.ylabel('TMAX Promedio (ºC)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)

    # 3. GUARDAR GRÁFICO EN UN BÚFER Y SUBIR A S3
    img_data = BytesIO()
    plt.savefig(img_data, format='png')
    img_data.seek(0)

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=PNG_DEST_KEY,
        Body=img_data,
        ContentType='image/png'
    )

    print(f" Job finalizado. Gráfico disponible en: s3://{BUCKET_NAME}/{PNG_DEST_KEY}")

if __name__ == "__main__":
    run_glue_job()
