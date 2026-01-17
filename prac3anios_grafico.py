import os
import subprocess
import sys

# 1. CONFIGURACIÓN DE ENTORNO
# Redirigimos todas las cachés y configuraciones a /tmp
os.environ['MPLCONFIGDIR'] = '/tmp/matplotlib_config'
os.environ['PYTHONCACHEPREFIX'] = '/tmp/pycache'
os.environ['PIP_CACHE_DIR'] = '/tmp/pip_cache'

# Crear los directorios manualmente para asegurar que existen
os.makedirs('/tmp/matplotlib_config', exist_ok=True)
os.makedirs('/tmp/pycache', exist_ok=True)
os.makedirs('/tmp/pip_cache', exist_ok=True)

# 2. INSTALACIÓN DE DEPENDENCIAS
try:
    print("Instalando dependencias en directorio temporal...")
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", 
        "--no-cache-dir", 
        "--upgrade", "numpy==1.23.5", "matplotlib"
    ])
    print("Dependencias instaladas correctamente.")
except Exception as e:
    print(f"Error instalando librerías: {e}")

# 3. IMPORTACIONES RESTANTES
import boto3
import pandas as pd
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
from io import BytesIO, StringIO

# --- CONFIGURACIÓN DE S3 ---
BUCKET_NAME = 'bigdata-weather-user4688629'
CSV_FOLDER_PREFIX = 'processed/TMAX_avg_from_year_files/'
PNG_DEST_KEY = 'visuals/grafico_tmax_anual_global.png'

s3_client = boto3.client('s3')

def run_visualization_job():
    print("Iniciando proceso de visualización...")

    # Localizar el archivo CSV (part-...)
    objetos = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=CSV_FOLDER_PREFIX)
    csv_key = None
    if 'Contents' in objetos:
        for obj in objetos['Contents']:
            if obj['Key'].endswith('.csv') and 'part-' in obj['Key']:
                csv_key = obj['Key']
                break

    if not csv_key:
        print("Error: No se encontró el archivo CSV de datos.")
        return

    # Leer datos
    print(f"Procesando: {csv_key}")
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=csv_key)
    df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
    df = df.sort_values('year')

    # 4. GRAFICAR
    plt.figure(figsize=(12, 6))
    plt.plot(df['year'], df['avg_temp'], marker='o', color='darkred', linestyle='-', label='TMAX Media Global')
    
    plt.title('Promedio Anual de Temperaturas Máximas (GHCNd Global)', fontsize=14)
    plt.xlabel('Año', fontsize=12)
    plt.ylabel('Temperatura (ºC)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()

    # 5. GUARDAR Y SUBIR
    img_data = BytesIO()
    plt.savefig(img_data, format='png', bbox_inches='tight')
    img_data.seek(0)

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=PNG_DEST_KEY,
        Body=img_data,
        ContentType='image/png'
    )

    print(f"Éxito. Gráfico guardado en: s3://{BUCKET_NAME}/{PNG_DEST_KEY}")

if __name__ == "__main__":
    run_visualization_job()