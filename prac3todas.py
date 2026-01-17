from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import Row

# 1. Sesión de spark:
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# 2. Cargar todas las estaciones (sus .csv) como un único rdd:
s3_input_path = "s3://noaa-ghcn-pds/csv/by_station/*.csv"
#s3_input_path = "s3://noaa-ghcn-pds/csv/by_station/USW00094728.csv"
rdd = sc.textFile(s3_input_path).coalesce(480)  # Creo el rdd. Reparticiono a 480 particiones para mejorar paralelismo.

# 3. Parsear líneas del csv:
def parse_line(line):
    fields = line.split(",")
    try:
        date_year = int(fields[1][0:4])  # Capto el año: si fields[1] es "DATE" (cabecera), int() lanza error y se descarta la línea.
        if fields[2] != "TMAX":
            return None  # Descarto filas que no son TMAX.
        elif fields[3] == "":
            return None  # Descarto DATA_VALUE vacíos.
        elif fields[5] != "":
            return None  # Descarto filas que no pasaron control de calidad automático.
        else:
            return (date_year, float(fields[3])/10)  # Devuelve tuplas de año y grados centígrados (divido por 10 la temperatura para ello).
    except (ValueError, IndexError):
        return None

parsed_rdd = rdd.map(parse_line) \
                .filter(lambda x: x is not None)

# 4. Año como clave y contador de temperaturas:
year_temp_pairs = parsed_rdd.map(lambda x: (x[0], (x[1], 1)))  # map actúa por cada fila: (año, temperatura) pasa a (año, (temperatura, 1))

# 5. Reduzco por clave (año), por cada año calculo (suma de temperaturas, suma de unidades) -> media por año y finalmente ordeno por año:
yearly_avg_rdd = year_temp_pairs \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: x[0] / x[1]) \
    .sortByKey()

# 6. Guardar resultados directamente en S3:
s3_output_path = "s3://bigdata-weather-user4688629/processed/TMAX_yearly_avg_all_stations"
yearly_avg_df = yearly_avg_rdd.map(lambda x: Row(year=x[0], avg_temp=x[1])).toDF()
yearly_avg_df.coalesce(1).write.mode("overwrite").csv(s3_output_path, header=True)

# 7. Parar Spark:
spark.stop()
