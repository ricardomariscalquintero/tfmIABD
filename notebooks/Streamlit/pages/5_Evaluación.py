import streamlit as st
import pandas as pd
from pyspark.sql.functions import col

st.title("Evaluación del jugador en centipawns")

# --- Verificaciones previas ---
if 'df_partidas' not in st.session_state:
    st.error("Primero debes visitar la página principal para cargar los datos.")
    st.stop()

if 'jugador_activo' not in st.session_state or not st.session_state.mostrar_estudio:
    st.warning("Primero selecciona un jugador en la sección de selección.")
    st.stop()

# --- Filtrar partidas del jugador activo y convertir a Pandas ---
df = st.session_state.df_partidas
nombre, fide_id = st.session_state.jugador_activo.split(" - ")

partidas = df.filter(
    (col("jugador") == nombre) & (col("fide_id").cast("string") == fide_id)
).toPandas()

if partidas.empty:
    st.warning("No se encontraron partidas para este jugador.")
    st.stop()

# --- Fases y columnas a analizar ---
fases = {
    "Apertura": "evaluacion_cp_apertura",
    "Mediojuego": "evaluacion_cp_mediojuego",
    "Final": "evaluacion_cp_final"
}

# --- Estadísticas generales por fase y color ---
st.subheader("Estadísticas generales por fase y color")
estadisticas = []

for fase, columna in fases.items():
    for color in ['W', 'B']:
        valores = partidas.loc[partidas['color'] == color, columna].dropna()
        if not valores.empty:
            estadisticas.append({
                "Fase": fase,
                "Color": "Blancas" if color == "W" else "Negras",
                "Media (cp)": round(valores.mean(), 2),
                "Desviación estándar": round(valores.std(), 2),
                "Partidas": len(valores)
            })

st.dataframe(pd.DataFrame(estadisticas), use_container_width=True)

# --- Mejores y peores evaluaciones por fase ---
st.subheader("Mejores y peores partidas por evaluación media en cada fase")

for fase, columna in fases.items():
    st.markdown(f"<h3 style='color:#B0B0B0;'>{fase}</h3>", unsafe_allow_html=True)
    partidas_fase = partidas.dropna(subset=[columna])
    
    if partidas_fase.empty:
        st.warning(f"No hay datos disponibles para la fase de {fase.lower()}.")
        continue

    col1, col2 = st.columns(2)

    # Top 3 mejores
    mejores = partidas_fase.sort_values(by=columna, ascending=False).head(3)
    with col1:
        st.markdown("<h5 style='color:#5DADE2;'>Mejor evaluación:</h5>", unsafe_allow_html=True)
        for _, row in mejores.iterrows():
            info = f"{row['evento']} – {row['lugar']} ({row['fechas'].date()})"
            valor = round(row[columna], 2)
            st.markdown(f"- `{valor} cp` – {info}")

    # Top 3 peores
    peores = partidas_fase.sort_values(by=columna, ascending=True).head(3)
    with col2:
        st.markdown("<h5 style='color:#EC7063;'>Peor evaluación:</h5>", unsafe_allow_html=True)
        for _, row in peores.iterrows():
            info = f"{row['evento']} – {row['lugar']} ({row['fechas'].date()})"
            valor = round(row[columna], 2)
            st.markdown(f"- `{valor} cp` – {info}")
