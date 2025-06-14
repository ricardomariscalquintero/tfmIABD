import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql.functions import col
from scipy.interpolate import make_interp_spline

st.title("Estadísticas del jugador")

# --- Verificación de datos cargados ---
if 'df_partidas' not in st.session_state:
    st.error("Primero debes visitar la página principal para cargar los datos.")
    st.stop()

if 'jugador_activo' not in st.session_state or not st.session_state.mostrar_estudio:
    st.warning("Primero selecciona un jugador en la sección de selección.")
    st.stop()

df = st.session_state.df_partidas
nombre, fide_id = st.session_state.jugador_activo.split(" - ")

# --- Filtrar y convertir a Pandas para análisis temporal ---
partidas = df.filter(
    (col("jugador") == nombre) & (col("fide_id").cast("string") == fide_id)
).toPandas()

if partidas.empty:
    st.warning("No se encontraron partidas para este jugador.")
    st.stop()

# --- Conversión de fechas y limpieza ---
partidas['fechas'] = pd.to_datetime(partidas['fechas'], errors='coerce')
partidas = partidas.dropna(subset=['fechas']).sort_values('fechas')

# --- Métricas generales ---
total = len(partidas)
blancas = (partidas['color'] == 'W').sum()
negras = (partidas['color'] == 'B').sum()
victorias = (partidas['resultados'] == 1.0).sum()
derrotas = (partidas['resultados'] == 0.0).sum()
tablas = (partidas['resultados'] == 0.5).sum()
elo_rivales = partidas['elo'].mean()

# --- Mostrar métricas ---
col1, col2, col3 = st.columns(3)
col1.metric("Total partidas", total)
col2.metric("Blancas", blancas)
col3.metric("Negras", negras)

col4, col5, col6 = st.columns(3)
col4.metric("Victorias", f"{victorias} ({victorias / total:.1%})")
col5.metric("Derrotas", f"{derrotas} ({derrotas / total:.1%})")
col6.metric("Tablas", f"{tablas} ({tablas / total:.1%})")

st.markdown(f"**ELO promedio de los rivales**: {elo_rivales:.0f}")

# --- Análisis por color ---
st.subheader("Resultados por color")

conteos = {
    "Victorias": {
        "Blancas": ((partidas['color'] == 'W') & (partidas['resultados'] == 1.0)).sum(),
        "Negras": ((partidas['color'] == 'B') & (partidas['resultados'] == 1.0)).sum()
    },
    "Tablas": {
        "Blancas": ((partidas['color'] == 'W') & (partidas['resultados'] == 0.5)).sum(),
        "Negras": ((partidas['color'] == 'B') & (partidas['resultados'] == 0.5)).sum()
    },
    "Derrotas": {
        "Blancas": ((partidas['color'] == 'W') & (partidas['resultados'] == 0.0)).sum(),
        "Negras": ((partidas['color'] == 'B') & (partidas['resultados'] == 0.0)).sum()
    }
}

# Totales por color para porcentajes
total_blancas = blancas if blancas > 0 else 1
total_negras = negras if negras > 0 else 1

tabla = pd.DataFrame({
    "Blancas": [
        f"{conteos['Victorias']['Blancas']} ({conteos['Victorias']['Blancas']/total_blancas:.1%})",
        f"{conteos['Tablas']['Blancas']} ({conteos['Tablas']['Blancas']/total_blancas:.1%})",
        f"{conteos['Derrotas']['Blancas']} ({conteos['Derrotas']['Blancas']/total_blancas:.1%})"
    ],
    "Negras": [
        f"{conteos['Victorias']['Negras']} ({conteos['Victorias']['Negras']/total_negras:.1%})",
        f"{conteos['Tablas']['Negras']} ({conteos['Tablas']['Negras']/total_negras:.1%})",
        f"{conteos['Derrotas']['Negras']} ({conteos['Derrotas']['Negras']/total_negras:.1%})"
    ]
}, index=["Victorias", "Tablas", "Derrotas"])

st.dataframe(tabla, use_container_width=True)

# --- Media móvil y evolución temporal ---
window = st.slider("Tamaño de la media móvil ", min_value=2, max_value=15, value=5, step=1)

fechas_inicio = partidas["fechas"].min()
fechas_fin = partidas["fechas"].max()
fechas_referencia = pd.date_range(start=fechas_inicio, end=fechas_fin, periods=60)
x_base = fechas_referencia.astype(np.int64)

st.markdown("### Evolución del rendimiento")
colores = {"W": "darkkhaki", "B": "slategray"}
fig, ax = plt.subplots(figsize=(10, 5))

for color in ["W", "B"]:
    datos = partidas[partidas["color"] == color].copy()
    if len(datos) < window:
        continue

    datos = datos.sort_values("fechas")
    datos["media_movil"] = datos["resultados"].rolling(window=window, min_periods=1).mean()
    media_por_dia = datos.groupby("fechas")["media_movil"].mean()
    interpolada = media_por_dia.reindex(fechas_referencia, method="nearest").interpolate()

    x = x_base
    y = interpolada.values

    if len(x) >= 4:
        spline = make_interp_spline(x, y, k=3)
        x_nuevo = np.linspace(x.min(), x.max(), 400)
        y_suave = spline(x_nuevo)
        y_suave = np.clip(y_suave, 0, 1)
        fechas_suaves = pd.to_datetime(x_nuevo)

        ax.plot(fechas_suaves, y_suave, label="Blancas" if color == "W" else "Negras",
                color=colores[color], linewidth=2)

        ax.text(fechas_suaves[-1] + pd.Timedelta(days=7), y_suave[-1],
                f"{y_suave[-1]:.2f}", fontsize=11, color=colores[color],
                va='center', ha='left', clip_on=False)

# --- Estética final ---
ax.set_title(f"Evolución del rendimiento de {nombre}", fontsize=16, weight='bold')
ax.set_ylim(0, 1.05)
ax.axhline(0.5, color="gray", linestyle="--", linewidth=0.8, alpha=0.5)
ax.legend()
ax.grid(False)
ax.tick_params(labelsize=10)
for spine in ax.spines.values():
    spine.set_visible(False)

st.pyplot(fig)
