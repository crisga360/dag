from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("PreprocessData") \
    .getOrCreate()

# Cargar datos preprocesados
movies_df = spark.read.parquet("/ruta/a/movies.parquet")
ratings_df = spark.read.parquet("/ruta/a/ratings.parquet")

# Realizar operaciones de preprocesamiento

# Guardar datos preprocesados en formato Parquet
preprocessed_data_df.write.mode("overwrite").parquet("/ruta/a/preprocessed_data.parquet")

# Cerrar la sesión de Spark
spark.stop()

