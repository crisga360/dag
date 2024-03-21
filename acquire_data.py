from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("AcquireData") \
    .getOrCreate()

# Cargar datos de películas y usuarios
movies_df = spark.read.format("csv").load("/ruta/a/peliculas.csv", header=True)
ratings_df = spark.read.format("csv").load("/ruta/a/calificaciones.csv", header=True)

# Realizar operaciones de limpieza y transformación si es necesario

# Guardar datos en formato Parquet para un procesamiento eficiente
movies_df.write.mode("overwrite").parquet("/ruta/a/movies.parquet")
ratings_df.write.mode("overwrite").parquet("/ruta/a/ratings.parquet")

# Cerrar la sesión de Spark
spark.stop()

