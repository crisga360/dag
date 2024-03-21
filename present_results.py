from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("PresentResults") \
    .getOrCreate()

# Cargar resultados del análisis
recommendations_df = spark.read.parquet("/ruta/a/recommendations.parquet")

# Presentar resultados al usuario

# Cerrar la sesión de Spark
spark.stop()

