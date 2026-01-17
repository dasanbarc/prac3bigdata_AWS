import pandas as pd
import boto3
import io
import pymysql

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'bigdata-weather-user4688629'
    
    # Rutas directas a las carpetas
    path_stations = 'processed/TMAX_yearly_avg_all_stations/'
    path_years = 'processed/TMAX_avg_from_year_files/'
    
    def get_single_csv(prefix):
        # Listamos el contenido de la ruta
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        # Buscamos archivos csv
        files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
        
        # Verificamos si la lista tiene contenido antes de acceder al índice [0]
        if not files:
            raise Exception(f"No se encontró el CSV en: {prefix}")
        
        file_key = files[0]
        print(f"Leyendo: {file_key}")
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        return pd.read_csv(io.BytesIO(obj['Body'].read()))

    try:
        # 1. Cargar los dos DataFrames
        df_stations = get_single_csv(path_stations)
        df_years = get_single_csv(path_years)
        
        # 2. Unir ambos por la columna year (Inner Join )
        merged = pd.merge(
            df_stations, 
            df_years, 
            on='year', 
            how='inner', 
            suffixes=('_from_stations', '_from_years')
        )
        
        if merged.empty:
            raise Exception("El cruce de datos (join) está vacío.")

        # 3. Comparación del redondeo a 2 decimales
        margen = 0.05  # Tolerancia para comparación
        merged['valido'] = (
            (merged['avg_temp_from_stations'] - merged['avg_temp_from_years']).abs() <= margen
        )
        
        # 4. Calcular porcentaje de filas que cumplen
        total = len(merged)
        correctos = merged['valido'].sum()
        precision = (correctos / total) * 100
        
        print(f"Resultado: {correctos} de {total} filas coinciden ({precision:.2f}%)")

        # 5. Poblar RDS
        try:
            host = "yearly-avg-tmax-from-noaa-stations-and-years.c3wbhtyroxmg.us-east-1.rds.amazonaws.com"
            conn = pymysql.connect(host=host, user="uoc_dasanbar", password="sdlgjehlihg934", connect_timeout=10)
            with conn.cursor() as cur:
                # 1. Crear y usar base de datos
                cur.execute("CREATE DATABASE IF NOT EXISTS yearly_avg_tmx")
                cur.execute("USE yearly_avg_tmx")

                # 2. Crear tabla from_stations y poblarla
                cur.execute("DROP TABLE IF EXISTS from_stations")
                cur.execute("CREATE TABLE from_stations (year INT, avg_temp FLOAT)")
                
                # Convertimos el DataFrame df_stations a lista de tuplas
                stats_data = [tuple(x) for x in df_stations[['year', 'avg_temp']].values]
                cur.executemany("INSERT INTO from_stations (year, avg_temp) VALUES (%s, %s)", stats_data)
                print(f"Tabla 'from_stations' poblada con {len(stats_data)} registros.")

                # 3. Crear tabla from_years y poblarla
                cur.execute("DROP TABLE IF EXISTS from_years")
                cur.execute("CREATE TABLE from_years (year INT, avg_temp FLOAT)")
                
                # Convertimos el DataFrame df_years a lista de tuplas
                years_data = [tuple(x) for x in df_years[['year', 'avg_temp']].values]
                cur.executemany("INSERT INTO from_years (year, avg_temp) VALUES (%s, %s)", years_data)
                print(f"Tabla 'from_years' poblada con {len(years_data)} registros.")

                conn.commit()
            conn.close()
            print("Poblado de RDS completado con éxito.")

        except Exception as e:
            print(f"Error al poblar tablas en RDS: {e}")

        # 6. Veredicto final (umbral 90%)
        if precision >= 90.0:
            mensaje_exito = f"Precisión del {precision:.2f}% detectada sobre {total} años."
            print(mensaje_exito)
            return mensaje_exito
        else:
            mensaje_error = f"SUSPENDIDO: La precisión ({precision:.2f}%) es inferior al 99% requerido."
            print(mensaje_error)
            raise Exception(mensaje_error)

    except Exception as e:
        print(f"Error: {str(e)}")
        raise e