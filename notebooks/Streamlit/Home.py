import streamlit as st
from pyspark.sql import SparkSession
import os

st.set_page_config(page_title="Estudio de Jugadores", layout="wide")
st.title("Analiza a tu rival")

st.image("ChessSet.jpg", width=300)


# Iniciar SparkSession solo una vez
if 'spark' not in st.session_state:
   st.session_state.spark = SparkSession.builder.appName("TFM_Analisis_jugadores").getOrCreate()
#    st.session_state.spark = (
#        SparkSession.builder
#        .appName("TFM_Analisis_jugadores")
#        .master("local[*]")  
#        .getOrCreate()
#    )

spark = st.session_state.spark

# Cargar datos y almacenarlos en session_state si no están ya cargados
if 'df_partidas' not in st.session_state:
    try:
        ruta_parquets_hdfs = "hdfs:///user/ajedrez/partidas/analizadas_grupo"
        df = spark.read.parquet(ruta_parquets_hdfs).cache()

        from pyspark.sql.functions import concat_ws, col
        df = df.withColumn("nombre_completo", concat_ws(" - ", col("jugador"), col("fide_id").cast("string")))

        st.session_state.df_partidas = df

    except Exception as e:
        st.error(f"No se pudieron cargar los datos desde HDFS: {e}")


st.markdown("Usa el menú lateral para comenzar.")
