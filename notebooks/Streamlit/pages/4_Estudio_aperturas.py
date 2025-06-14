import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import re

df = st.session_state.df_partidas
nombre, fide_id = st.session_state.jugador_activo.split(" - ")

st.title(f"Estudio de las aperturas: {nombre}")

# --- Verificaciones previas ---
if 'df_partidas' not in st.session_state:
    st.error("Primero debes visitar la página principal para cargar los datos.")
    st.stop()

if 'jugador_activo' not in st.session_state or not st.session_state.mostrar_estudio:
    st.warning("Primero selecciona un jugador en la sección de selección.")
    st.stop()



# --- Filtrar partidas del jugador y convertir a Pandas ---
partidas = df.filter(
    (df["jugador"] == nombre) & (df["fide_id"].cast("string") == fide_id)
).toPandas()

if partidas.empty:
    st.warning("No se encontraron partidas para este jugador.")
    st.stop()

# --- Extraer campos desde PGN en Pandas ---
def extraer_eco(pgn):
    if pd.isna(pgn):
        return "Desconocido"
    match = re.search(r'\[ECO \"(.*?)\"\]', pgn)
    return match.group(1) if match else "Desconocido"

def extraer_opening(pgn):
    if pd.isna(pgn):
        return "Desconocido"
    match = re.search(r'\[Opening \"(.*?)\"\]', pgn)
    return match.group(1) if match else "Desconocido"

partidas['eco'] = partidas['pgn'].apply(extraer_eco)
partidas['opening'] = partidas['pgn'].apply(extraer_opening)

# --- Mostrar estadísticas por color ---
col1, col2 = st.columns(2)
for color, col_ui in zip(['W', 'B'], [col1, col2]):
    partidas_color = partidas[partidas['color'] == color]
    if partidas_color.empty:
        col_ui.warning(f"No hay partidas como {'Blancas' if color == 'W' else 'Negras'}.")
        continue

    total = len(partidas_color)
    resumen = partidas_color.groupby('opening').agg(
        Partidas=('opening', 'count'),
        Resultado_medio=('resultados', 'mean')
    ).sort_values('Partidas', ascending=False)

    resumen['Frecuencia'] = (resumen['Partidas'] / total * 100).round(1).astype(str) + '%'
    resumen['Resultado_medio'] = resumen['Resultado_medio'].round(2)

    col_ui.markdown(f"### {'Blancas' if color == 'W' else 'Negras'}")
    col_ui.dataframe(resumen.head(10), use_container_width=True)

    # --- Gráfico de barras horizontal ---
    resumen_plot = resumen.head(10).sort_values('Partidas', ascending=True)
    fig, ax = plt.subplots(figsize=(6, 4))
    color_barras = "darkkhaki" if color == "W" else "slategray"
    ax.barh(resumen_plot.index, resumen_plot['Partidas'], color=color_barras)
    ax.set_title(f"Aperturas más frecuentes ({'Blancas' if color == 'W' else 'Negras'})")
    ax.set_xlabel("Número de partidas")
    ax.set_ylabel("Apertura")
    ax.grid(axis='x', linestyle='--', alpha=0.5)
    col_ui.pyplot(fig)
