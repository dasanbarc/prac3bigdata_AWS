from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

# 1. Sesión de Spark:
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# 2. Cargar los archivos CSV organizados por año:
# Usamos el comodín *.csv para leer todos los años disponibles
s3_input_path = "s3://noaa-ghcn-pds/csv/by_year/*.csv"
rdd = sc.textFile(s3_input_path, minPartitions=480)  # Creo el rdd. Aumento particiones para mejor paralelismo

# 3. Parsear líneas del csv:
def parse_line(line):
    fields = line.split(",")
    try:
        # En los archivos 'by_year', la columna 1 sigue siendo la fecha YYYYMMDD
        date_str = fields[1]
        year = int(date_str[0:4])
        
        # Filtros de calidad y tipo de dato
        if fields[2] != "TMAX":
            return None  # Solo nos interesa la temperatura máxima
        if fields[3] == "":
            return None  # Saltar valores vacíos
        if len(fields) > 5 and fields[5] != "":
            return None  # Q_FLAG: Descartar si no pasó el control de calidad
            
        # El valor viene en décimas de grado Celsius
        temp_celsius = float(fields[3]) / 10
        return (year, temp_celsius)
        
    except (ValueError, IndexError):
        # Captura cabeceras o líneas mal formateadas
        return None

# Aplicar transformación y filtrado
parsed_rdd = rdd.map(parse_line).filter(lambda x: x is not None)

# 4. Preparar para el cálculo: (año, (temperatura, 1))
year_temp_pairs = parsed_rdd.map(lambda x: (x[0], (x[1], 1)))

# 5. Reducción y cálculo de la media:
# Sumamos las temperaturas y el contador por cada año, luego dividimos.
yearly_avg_rdd = year_temp_pairs \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: x[0] / x[1]) \
    .sortByKey()

# 6. Guardar resultados en S3:
# Cambiamos el nombre de la carpeta de salida para diferenciar el proceso
s3_output_path = "s3://bigdata-weather-user4688629/processed/TMAX_avg_from_year_files"
yearly_avg_df = yearly_avg_rdd.map(lambda x: Row(year=x[0], avg_temp=x[1])).toDF()

# Coalesce(1) genera un solo archivo CSV de salida
yearly_avg_df.coalesce(1).write.mode("overwrite").csv(s3_output_path, header=True)

# 7. Parar Spark:
spark.stop()