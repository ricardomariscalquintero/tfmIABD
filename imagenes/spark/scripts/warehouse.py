#Importamos las librerías de SparkSQL.
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, date_format,
    row_number, trim, coalesce, lit, when
)
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
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
    .withColumn("nombre_mes", date_format("fecha", "MMMM"))

windowFecha = Window.orderBy("fecha")

if modoCarga == "inicial":
    #Como es inicial, se escribe completamente la dimensión.
    dfFechas = dfFechas.withColumn("id_fecha", row_number().over(windowFecha))
    dfFechas.write.mode("overwrite").saveAsTable("dim_fecha")
else:
    #Mediante el left_anti join conseguimos detectar que fechas están en el nuevo
    #dataframe que no está en dim_fecha. Una vez que los tengo localizados le hago
    #un append a la dimensión fecha con los nuevos.
    #Utilizo alias n(nuevos) y e(existentes) para que sea sencillo trabajar con
    #los datos.
    nuevasFechas = dfFechas.alias("n").join(
        spark.table("dim_fecha").alias("e"), on="fecha", how="left_anti"
    ).withColumn("id_fecha", row_number().over(windowFecha))
    nuevasFechas.write.mode("append").saveAsTable("dim_fecha")

#=======================
#Dimensión Jugador
#=======================
#Creamos dos dataframes con los datos de los jugadores.
white = df.select(
    col("White").alias("nombre"),
    col("WhiteFideId").alias("fide_id")
).filter(col("fide_id").isNotNull())

black = df.select(
    col("Black").alias("nombre"),
    col("BlackFideId").alias("fide_id")
).filter(col("fide_id").isNotNull())

#Unimos los dos dataframes en uno que sería la dimensión de jugadores.
dimJugador = white.union(black).dropDuplicates(["fide_id"])

windowJugador = Window.orderBy("fide_id")

#Actuamos de igual forma que anteriormente.
if modoCarga == "inicial":
    dimJugador = dimJugador.withColumn("id_jugador", row_number().over(windowJugador))
    dimJugador.write.mode("overwrite").saveAsTable("dim_jugador")
else:
    nuevosJugadores = dimJugador.alias("n").join(
        spark.table("dim_jugador").alias("e"),
        on="fide_id", how="left_anti"
    ).withColumn("id_jugador", row_number().over(windowJugador))
    nuevosJugadores.write.mode("append").saveAsTable("dim_jugador")

#=======================
#Dimensión Perfil de jugador
#=======================

#Creamos los perfiles de los usuarios.
perfilesWhite = df.select(
    col("WhiteTitle").alias("titulo"),
    col("WhiteTituloNombre").alias("titulo_descripcion"),
    col("WhiteGenderTitle").alias("genero_titulo"),
    col("WhiteHasTitle").alias("tiene_titulo")
)

perfilesBlack = df.select(
    col("BlackTitle").alias("titulo"),
    col("BlackTituloNombre").alias("titulo_descripcion"),
    col("BlackGenderTitle").alias("genero_titulo"),
    col("BlackHasTitle").alias("tiene_titulo")
)

dimPerfil = perfilesWhite.union(perfilesBlack).dropDuplicates()

windowPerfil = Window.orderBy("titulo", "titulo_descripcion", "genero_titulo", "tiene_titulo")

if modoCarga == "inicial":
    dimPerfil = dimPerfil.withColumn("id_perfil", row_number().over(windowPerfil))
    dimPerfil.write.mode("overwrite").saveAsTable("dim_perfil")
else:
    nuevosPerfiles = dimPerfil.alias("n").join(
        spark.table("dim_perfil").alias("e"),
        on=["titulo", "titulo_descripcion", "genero_titulo", "tiene_titulo"],
        how="left_anti"
    ).withColumn("id_perfil", row_number().over(windowPerfil))
    
    nuevosPerfiles.write.mode("append").saveAsTable("dim_perfil")

#=======================
#Dimensión Evento
#=======================
#Creamos el dataframe con los campos relacionados.
#Además, borramos los duplicados.
df = df.withColumn("Event", trim(col("Event")))

#Seleccionamos directamente las columnas que nos interesan.
dimEvento = df.na.drop(subset=["Event"]).select(
    "Event",
    "Ciudad",
    "PaisISO3",
    "Pais",
    "Latitud",
    "Longitud",
    "Site"
).dropDuplicates()

windowEvento = Window.orderBy("Event")

if modoCarga == "inicial":
    dimEvento = dimEvento.withColumn("id_evento", row_number().over(windowEvento))
    dimEvento.write.mode("overwrite").saveAsTable("dim_evento")
else:
    nuevosEventos = dimEvento.alias("n").join(
        spark.table("dim_evento").alias("e"), on="Event", how="left_anti"
    ).withColumn("id_evento", row_number().over(windowEvento))
    nuevosEventos.write.mode("append").saveAsTable("dim_evento")

#=======================
#Dimensión Apertura
#=======================
#Realizamos una limpieza de los campos para evitar los errores al realizar el join.
df = df.withColumn("ECO", trim(coalesce(col("ECO"), lit("DESC")))) \
       .withColumn("Opening", trim(coalesce(col("Opening"), lit("Desconocida"))))

#Eliminamos las filas con duplicados.
dimApertura = df.select("ECO", "Opening").dropDuplicates(["ECO", "Opening"])

windowApertura = Window.orderBy("ECO", "Opening")

if modoCarga == "inicial":
    dimApertura = dimApertura.withColumn("id_apertura", row_number().over(windowApertura))
    dimApertura.write.mode("overwrite").saveAsTable("dim_apertura")
else:
    nuevasAperturas = dimApertura.alias("n").join(
        spark.table("dim_apertura").alias("e"),
        on=["ECO", "Opening"], how="left_anti"
    ).withColumn("id_apertura", row_number().over(windowApertura))
    nuevasAperturas.write.mode("append").saveAsTable("dim_apertura")

#=======================
#Hecho Partida
#=======================
#Cargamos las dimensiones.
dimJugador  = spark.table("dim_jugador")
dimPerfil   = spark.table("dim_perfil")
dimFecha    = spark.table("dim_fecha")
dimEvento   = spark.table("dim_evento")
dimApertura = spark.table("dim_apertura")

#Creamos la tabla de hechos. Para ello, generamos un dataframe con las tuplas de
#las dimensiones y posteriormente seleccionamos los campos.
hechoPartida = df \
    .filter(col("WhiteFideId").isNotNull() & col("BlackFideId").isNotNull()) \
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
    .join(dimEvento, on="Event", how="left") \
    .join(dimApertura, on=["ECO", "Opening"], how="left") \
    .join(
        dimPerfil.select(
            col("id_perfil"),
            col("titulo").alias("WhiteTitle"),
            col("titulo_descripcion").alias("WhiteTituloNombre"),
            col("genero_titulo").alias("WhiteGenderTitle"),
            col("tiene_titulo").alias("WhiteHasTitle")
        ),
        on=["WhiteTitle", "WhiteTituloNombre", "WhiteGenderTitle", "WhiteHasTitle"],
        how="left"
    ) \
    .withColumnRenamed("id_perfil", "id_perfil_blanco") \
    .join(
        dimPerfil.select(
            col("id_perfil"),
            col("titulo").alias("BlackTitle"),
            col("titulo_descripcion").alias("BlackTituloNombre"),
            col("genero_titulo").alias("BlackGenderTitle"),
            col("tiene_titulo").alias("BlackHasTitle")
        ),
        on=["BlackTitle", "BlackTituloNombre", "BlackGenderTitle", "BlackHasTitle"],
        how="left"
    ) \
    .withColumnRenamed("id_perfil", "id_perfil_negro") \
    .withColumn("id_ganador", when(col("Ganador") == "White", col("id_jugador_blanco"))
                             .when(col("Ganador") == "Black", col("id_jugador_negro"))
                             .otherwise(None)) \
    .select(
        "id_jugador_blanco", "id_jugador_negro",
        "id_perfil_blanco", "id_perfil_negro",
        "id_fecha_partida", "id_fecha_evento",
        "id_evento", "id_apertura",
        "ResultadoBinario", "Ganador", "id_ganador",
        "DiferenciaELO", "Round", "MatchType",
        col("WhiteElo").cast("int").alias("elo_blanco"),
        col("BlackElo").cast("int").alias("elo_negro")
    )

#Eliminamos los registros duplicados.
hechoPartida = hechoPartida.dropDuplicates([
    "id_jugador_blanco", "id_jugador_negro",
    "id_perfil_blanco", "id_perfil_negro", 
    "id_fecha_partida", "id_evento", "Round", 
    "ResultadoBinario", "id_apertura"
])

if modoCarga == "inicial":
    hechoPartida = hechoPartida.withColumn(
        "id_partida", row_number().over(Window.orderBy("id_evento", "id_fecha_partida", "Round"))
    )
    hechoPartida.write.mode("overwrite").saveAsTable("hecho_partida")
else:
    nuevasPartidas = hechoPartida.alias("n").join(
        spark.table("hecho_partida").alias("e"),
        on=[
            "id_jugador_blanco", "id_jugador_negro", 
            "id_fecha_partida", "id_evento", "Round", 
            "ResultadoBinario", "id_apertura"
        ],
        how="left_anti"
    )

    nuevasPartidas = nuevasPartidas.withColumn(
        "id_partida", row_number().over(Window.orderBy("id_evento", "id_fecha_partida", "Round"))
    )

    nuevasPartidas.write.mode("append").saveAsTable("hecho_partida")

#=======================
#Renombramos el CSV procesado para que no se vuelva a procesar
#=======================
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
