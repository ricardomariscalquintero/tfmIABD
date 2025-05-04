from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, date_format, monotonically_increasing_id
)
import argparse

# Argumentos
parser = argparse.ArgumentParser()
parser.add_argument('--input', required=True)
args = parser.parse_args()

# SparkSession con Hive habilitado
spark = SparkSession.builder \
    .appName("Procesar Partidas Ajedrez desde HDFS") \
    .enableHiveSupport() \
    .getOrCreate()

# Crear base de datos si no existe
spark.sql("CREATE DATABASE IF NOT EXISTS ajedrez")
spark.catalog.setCurrentDatabase("ajedrez")

# Leer CSV desde HDFS
df = spark.read.option("header", True).option("inferSchema", True).csv(args.input)

# =======================
# Dimensión Fecha
# =======================
df = df.withColumn("FechaPartida", to_date("Date")) \
       .withColumn("FechaEvento", to_date("EventDate"))

df_fechas = df.select("FechaPartida").union(df.select("FechaEvento")).distinct() \
    .withColumnRenamed("FechaPartida", "fecha") \
    .withColumn("anio", year("fecha")) \
    .withColumn("mes", month("fecha")) \
    .withColumn("dia", dayofmonth("fecha")) \
    .withColumn("nombre_mes", date_format("fecha", "MMMM")) \
    .withColumn("id_fecha", monotonically_increasing_id())

df_fechas.write.mode("overwrite").saveAsTable("dim_fecha")

# =======================
# Dimensión Jugador
# =======================
white = df.select(
    col("White").alias("nombre"),
    col("WhiteElo").alias("elo"),
    col("WhiteTitle").alias("titulo"),
    col("WhiteTituloNombre").alias("titulo_descripcion"),
    col("WhiteGenderTitle").alias("genero_titulo"),
    col("WhiteHasTitle").alias("tiene_titulo"),
    col("WhiteFideId").alias("fide_id")
)
black = df.select(
    col("Black").alias("nombre"),
    col("BlackElo").alias("elo"),
    col("BlackTitle").alias("titulo"),
    col("BlackTituloNombre").alias("titulo_descripcion"),
    col("BlackGenderTitle").alias("genero_titulo"),
    col("BlackHasTitle").alias("tiene_titulo"),
    col("BlackFideId").alias("fide_id")
)
dim_jugador = white.union(black).dropDuplicates(["fide_id"]) \
    .withColumn("id_jugador", monotonically_increasing_id())

dim_jugador.write.mode("overwrite").saveAsTable("dim_jugador")

# =======================
# Dimensión Evento
# =======================
dim_evento = df.select(
    "Event", "Ciudad", "PaisISO3", "Pais", "Latitud", "Longitud", "Site", "MatchType"
).dropDuplicates() \
 .withColumn("id_evento", monotonically_increasing_id())

dim_evento.write.mode("overwrite").saveAsTable("dim_evento")

# =======================
# Dimensión Apertura
# =======================
dim_apertura = df.select("ECO", "Opening") \
    .dropDuplicates() \
    .withColumn("id_apertura", monotonically_increasing_id())

dim_apertura.write.mode("overwrite").saveAsTable("dim_apertura")

# =======================
# Hechos
# =======================
hechos = df \
    .join(dim_jugador.select("id_jugador", col("fide_id").alias("WhiteFideId")),
          on="WhiteFideId", how="left") \
    .withColumnRenamed("id_jugador", "id_jugador_blanco") \
    .join(dim_jugador.select("id_jugador", col("fide_id").alias("BlackFideId")),
          on="BlackFideId", how="left") \
    .withColumnRenamed("id_jugador", "id_jugador_negro") \
    .join(df_fechas.withColumnRenamed("fecha", "FechaPartida"),
          on="FechaPartida", how="left") \
    .withColumnRenamed("id_fecha", "id_fecha_partida") \
    .join(df_fechas.withColumnRenamed("fecha", "FechaEvento"),
          on="FechaEvento", how="left") \
    .withColumnRenamed("id_fecha", "id_fecha_evento") \
    .join(dim_evento,
          on=["Event", "Ciudad", "PaisISO3", "Pais", "Latitud", "Longitud", "Site", "MatchType"],
          how="left") \
    .join(dim_apertura,
          on=["ECO", "Opening"],
          how="left") \
    .select(
        monotonically_increasing_id().alias("id_partida"),
        "id_jugador_blanco", "id_jugador_negro",
        "id_fecha_partida", "id_fecha_evento",
        "id_evento", "id_apertura",
        "ResultadoBinario", "Ganador", "DiferenciaELO", "Round"
    )

hechos.write.mode("overwrite").saveAsTable("hechos")

spark.stop()

