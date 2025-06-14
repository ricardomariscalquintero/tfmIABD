import streamlit as st
import pandas as pd
from pyspark.sql.functions import col

df = st.session_state.df_partidas
nombre, fide_id = st.session_state.jugador_activo.split(" - ")

st.title(f"Errores graves por fase: {nombre}")

# --- Verificaciones previas ---
if 'df_partidas' not in st.session_state:
    st.error("Primero debes visitar la página principal para cargar los datos.")
    st.stop()

if 'jugador_activo' not in st.session_state or not st.session_state.mostrar_estudio:
    st.warning("Primero selecciona un jugador en la sección de selección.")
    st.stop()



# --- Filtrar partidas y convertir a Pandas ---
partidas = df.filter(
    (col("jugador") == nombre) & (col("fide_id").cast("string") == fide_id)
).toPandas()

if partidas.empty:
    st.warning("No se encontraron partidas para este jugador.")
    st.stop()

partidas['fechas'] = pd.to_datetime(partidas['fechas'], errors='coerce')

# --- Columnas por fase ---
errores_graves_fases = {
    "Apertura": "errores_graves_apertura",
    "Mediojuego": "errores_graves_mediojuego",
    "Final": "errores_graves_final"
}

# --- Mostrar en columnas 3x1 ---
col1, col2, col3 = st.columns(3)

for fase, col_ui, columna in zip(errores_graves_fases.keys(), [col1, col2, col3], errores_graves_fases.values()):
    partidas_fase = partidas[partidas[columna] > 0].copy()
    partidas_fase = partidas_fase.sort_values('fechas', ascending=False).head(3)

    col_ui.markdown(f"<h3 style='color:#B0B0B0;'>{fase}</h3>", unsafe_allow_html=True)
    if partidas_fase.empty:
        col_ui.warning("Sin errores graves recientes.")
        continue

    for _, row in partidas_fase.iterrows():
        identificador = f"{row['evento']} – {row['lugar']} ({row['fechas'].date()})"
        col_ui.markdown(f"- **{identificador}**: {int(row[columna])} errores graves")
