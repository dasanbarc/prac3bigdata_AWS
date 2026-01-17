import boto3
import pandas as pd
from io import StringIO

# --- CONFIGURACIÓN ---
BUCKET_NAME = 'bigdata-weather-user4688629'
YEARS_TO_FILTER = [1950, 1975, 1990, 2000, 2020]

# Definición de las rutas a comparar
SOURCES = {
    "by_year_files": "processed/TMAX_avg_from_year_files/",
    "by_station_files": "processed/TMAX_yearly_avg_all_stations/"
}

OUTPUT_PATH = "processed/final_check_5years/final_check_5years.csv"

s3_client = boto3.client('s3')

def get_csv_from_s3_folder(prefix):
    """Localiza y lee el archivo part-*.csv dentro de una carpeta de Spark."""
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    
    csv_key = next((obj['Key'] for obj in response.get('Contents', []) 
                   if obj['Key'].endswith('.csv') and 'part-' in obj['Key']), None)
    
    if not csv_key:
        raise FileNotFoundError(f"No se encontró el archivo CSV en s3://{BUCKET_NAME}/{prefix}")
    
    print(f"Leyendo: {csv_key}")
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=csv_key)
    return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

def run_consolidation():
    all_data = []

    for label, prefix in SOURCES.items():
        try:
            # 1. Cargar el DataFrame desde S3
            df = get_csv_from_s3_folder(prefix)
            
            # 2. Filtrar por los años seleccionados
            df_filtered = df[df['year'].isin(YEARS_TO_FILTER)].copy()
            
            # 3. Formatear columnas: year | origin | result
            df_filtered['origin'] = prefix
            df_filtered = df_filtered.rename(columns={'avg_temp': 'result'})
            
            # Reordenar columnas según lo solicitado
            all_data.append(df_filtered[['year', 'origin', 'result']])
            
        except Exception as e:
            print(f"Error procesando {label}: {e}")

    # 4. Concatenar resultados
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = final_df.sort_values(by=['year', 'origin'])
        
        print("\nVista previa del resultado final:")
        print(final_df)

        # 5. Guardar en S3
        csv_buffer = StringIO()
        final_df.to_csv(csv_buffer, index=False)
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=OUTPUT_PATH,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        print(f"\nArchivo guardado exitosamente en: s3://{BUCKET_NAME}/{OUTPUT_PATH}")
    else:
        print("No se pudieron recolectar datos.")

if __name__ == "__main__":
    run_consolidation()