#Importamos las librerías de SparkSQL.
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, date_format,
    monotonically_increasing_id
)
from pyspark.sql.utils import AnalysisException
from subprocess import call
from datetime import datetime
import argparse

#Recibimos los argumentos de entrada.
#En este caso sería la ruta del CSV para procesar.
parseador = argparse.ArgumentParser()
parseador.add_argument('--input', required=True)
argumentosEntrada = parseador.parse_args()

#Creamos el SparkSession con Hive habilitado.
spark = SparkSession.builder \
    .appName("ETL Partidas Ajedrez desde HDFS") \
    .enableHiveSupport() \
    .getOrCreate()

#Creamos la base de datos si no existe.
spark.sql("CREATE DATABASE IF NOT EXISTS ajedrez")
spark.catalog.setCurrentDatabase("ajedrez")

#Leemos el CSV desde HDFS.
df = spark.read.option("header", True).option("inferSchema", True).csv(argumentosEntrada.input)

#Detectamos si estamos realizando un ETL inicial o incremental.
try:
    if spark.table("hecho_partida").count() > 0:
        modoCarga = "incremental"
    else:
        modoCarga = "inicial"
        
except AnalysisException:
    modoCarga = "inicial"

print(f"Modo de carga detectado: {modoCarga}")

#=======================
#Dimensión Fecha
#=======================
df = df.withColumn("FechaPartida", to_date("Date")) \
       .withColumn("FechaEvento",  to_date("EventDate"))

dfFechas = df.select("FechaPartida").union(df.select("FechaEvento")).distinct() \
    .withColumnRenamed("FechaPartida", "fecha") \
    .withColumn("anio", year("fecha")) \
    .withColumn("mes", month("fecha")) \
    .withColumn("dia", dayofmonth("fecha")) \
    .withColumn("nombre_mes", date_format("fecha", "MMMM")) \
    .withColumn("id_fecha", monotonically_increasing_id())

if modoCarga == "inicial":
    #Como es inicial, se escribe completamente la dimensión.
    dfFechas.write.mode("overwrite").saveAsTable("dim_fecha")
else:
    #Mediante el left_anti join conseguimos detectar que fechas están en el nuevo
    #dataframe que no está en dim_fecha. Una vez que los tengo localizados le hago
    #un append a la dimensión fecha con los nuevos.
    #Utilizo alias n(nuevos) y e(existentes) para que sea sencillo trabajar con
    #los datos.
    dfFechas.alias("n").join(
        spark.table("dim_fecha").alias("e"), on="fecha", how="left_anti"
    ).withColumn("id_fecha", monotonically_increasing_id()) \
     .write.mode("append").saveAsTable("dim_fecha")

#=======================
#Dimensión Jugador
#=======================
#Creamos dos dataframes con los datos de los jugadores.
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

#Unimos los dos dataframes en uno que sería la dimensión de jugadores.
dimJugador = white.union(black).dropDuplicates(["fide_id"]) \
    .withColumn("id_jugador", monotonically_increasing_id())

#Actuamos de igual forma que anteriormente.
if modoCarga == "inicial":
    dimJugador.write.mode("overwrite").saveAsTable("dim_jugador")
else:
    dimJugador.alias("n").join(
        spark.table("dim_jugador").alias("e"), on="fide_id", how="left_anti"
    ).withColumn("id_jugador", monotonically_increasing_id()) \
     .write.mode("append").saveAsTable("dim_jugador")

#=======================
#Dimensión Evento
#=======================
#Creamos el dataframe con los campos relacionados.
#Además, borramos los duplicados.
dimEvento = df.select("Event", "Ciudad", "PaisISO3", "Pais", "Latitud", "Longitud", "Site", "MatchType") \
    .dropDuplicates() \
    .withColumn("id_evento", monotonically_increasing_id())

if modoCarga == "inicial":
    dimEvento.write.mode("overwrite").saveAsTable("dim_evento")
else:
    dimEvento.alias("n").join(
        spark.table("dim_evento").alias("e"),
        on=["Event", "Ciudad", "PaisISO3", "Pais", "Latitud", "Longitud", "Site", "MatchType"],
        how="left_anti"
    ).withColumn("id_evento", monotonically_increasing_id()) \
     .write.mode("append").saveAsTable("dim_evento")

#=======================
#Dimensión Apertura
#=======================
dimApertura = df.select("ECO", "Opening").dropDuplicates() \
    .withColumn("id_apertura", monotonically_increasing_id())

if modoCarga == "inicial":
    dimApertura.write.mode("overwrite").saveAsTable("dim_apertura")
else:
    dimApertura.alias("n").join(
        spark.table("dim_apertura").alias("e"),
        on=["ECO", "Opening"], how="left_anti"
    ).withColumn("id_apertura", monotonically_increasing_id()) \
     .write.mode("append").saveAsTable("dim_apertura")

#=======================
#Hecho Partida
#=======================
dimJugador  = spark.table("dim_jugador")
dimFecha    = spark.table("dim_fecha")
dimEvento   = spark.table("dim_evento")
dimApertura = spark.table("dim_apertura")

hechoPartida = df \
    .join(dimJugador.select("id_jugador", col("fide_id").alias("WhiteFideId")),
          on="WhiteFideId", how="left") \
    .withColumnRenamed("id_jugador", "id_jugador_blanco") \
    .join(dimJugador.select("id_jugador", col("fide_id").alias("BlackFideId")),
          on="BlackFideId", how="left") \
    .withColumnRenamed("id_jugador", "id_jugador_negro") \
    .join(dimFecha.withColumnRenamed("fecha", "FechaPartida"),
          on="FechaPartida", how="left") \
    .withColumnRenamed("id_fecha", "id_fecha_partida") \
    .join(dimFecha.withColumnRenamed("fecha", "FechaEvento"),
          on="FechaEvento", how="left") \
    .withColumnRenamed("id_fecha", "id_fecha_evento") \
    .join(dimEvento,
          on=["Event", "Ciudad", "PaisISO3", "Pais", "Latitud", "Longitud", "Site", "MatchType"],
          how="left") \
    .join(dimApertura,
          on=["ECO", "Opening"],
          how="left") \
    .select(
        monotonically_increasing_id().alias("id_partida"),
        "id_jugador_blanco", "id_jugador_negro",
        "id_fecha_partida", "id_fecha_evento",
        "id_evento", "id_apertura",
        "ResultadoBinario", "Ganador", "DiferenciaELO", "Round"
    )

if modoCarga == "inicial":
    hechoPartida.write.mode("overwrite").saveAsTable("hecho_partida")
else:
    hechoPartida.alias("n").join(
        spark.table("hecho_partida").alias("e"),
        on=["id_jugador_blanco", "id_jugador_negro", "id_fecha_partida", "id_evento", "Round"],
        how="left_anti"
    ).write.mode("append").saveAsTable("hecho_partida")

#=======================
#Renombramos el CSV procesado para que no se vuelva a procesar
# =======================
rutaOrigen  = argumentosEntrada.input
timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
rutaDestino = rutaOrigen.replace(".csv", f"_{timestamp}.csv")

resultado = call(["hdfs", "dfs", "-mv", rutaOrigen, rutaDestino])

if resultado == 0:
    print(f"Fichero CSV renombrado en HDFS: {rutaDestino}")
else:
    print(f"No se pudo renombrar el fichero: {rutaOrigen}")

# Finalizar
spark.stop()
