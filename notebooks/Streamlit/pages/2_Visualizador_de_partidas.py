import streamlit as st
from pyspark.sql.functions import col
import chess
import chess.svg
import cairosvg
import ast

def parse_jugadas(raw_san):
    if isinstance(raw_san, list):
        return raw_san
    if isinstance(raw_san, str):
        try:
            return ast.literal_eval(raw_san)
        except Exception:
            return []
    return []

# --- Verificar estado ---
if 'jugador_activo' not in st.session_state or not st.session_state.mostrar_estudio:
    st.warning("Primero selecciona un jugador en la sección anterior.")
    st.stop()

df = st.session_state.df_partidas
nombre, fide_id = st.session_state.jugador_activo.split(" - ")

# --- Filtrar partidas del jugador activo ---
partidas_filtradas = df.filter(
    (col("jugador") == nombre) & (col("fide_id").cast("string") == fide_id)
).toPandas()

if partidas_filtradas.empty:
    st.warning("No se encontraron partidas para este jugador.")
    st.stop()

# --- Conteo por color ---
num_blancas = (partidas_filtradas['color'] == 'W').sum()
num_negras = (partidas_filtradas['color'] == 'B').sum()

st.title("Visualizador de partidas")
st.info(f"{nombre} jugó:\n\n- {num_blancas} como **Blancas**\n- {num_negras} como **Negras**")

col1, col2 = st.columns(2)
with col1:
    if st.button(f"Estudiar con Blancas ({num_blancas})"):
        st.session_state.color_estudio = "W"
with col2:
    if st.button(f"Estudiar con Negras ({num_negras})"):
        st.session_state.color_estudio = "B"

if 'color_estudio' not in st.session_state or not st.session_state.color_estudio:
    st.stop()

# --- Procesar partidas válidas ---
partidas = partidas_filtradas[partidas_filtradas['color'] == st.session_state.color_estudio].copy()
partidas['jugadas'] = partidas['san'].apply(parse_jugadas)
partidas = partidas[partidas['jugadas'].apply(lambda j: isinstance(j, list) and len(j) > 0)].reset_index(drop=True)

# Usamos todo el dataset cargado como Pandas para buscar oponentes
df_full = df.toPandas()

def obtener_oponente(row):
    posibles = df_full[
        (df_full['fechas'] == row['fechas']) &
        (df_full['evento'] == row['evento']) &
        (df_full['color'] != row['color']) &
        (df_full['jugador'] != row['jugador'])
    ]

    return posibles['jugador'].values[0] if not posibles.empty else "???"

partidas['jugador_oponente'] = partidas.apply(obtener_oponente, axis=1)
partidas['etiqueta'] = partidas.apply(
    lambda row: f"{row['fechas'].date()} | {row['evento']} vs {row['jugador_oponente']} – {len(row['jugadas']) // 2} jugadas – Resultado: {row['resultados']}",
    axis=1
)

# --- Selección de partida ---
opciones = partidas['etiqueta'].tolist()
if not opciones:
    st.warning("No hay partidas disponibles con jugadas válidas.")
    st.stop()

seleccion = st.selectbox("Selecciona partida:", opciones)

idx = opciones.index(seleccion)
if 'partida_idx' not in st.session_state or idx != st.session_state.partida_idx:
    st.session_state.partida_idx = idx
    st.session_state.movimiento = -1

partida = partidas.iloc[st.session_state.partida_idx]
jugadas = partida["jugadas"]
total = len(jugadas)

# --- Navegación de jugadas ---
col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
with col1:
    if st.button("⏮️"):
        st.session_state.movimiento = -1
with col2:
    if st.button("⬅️"):
        if st.session_state.movimiento > -1:
            st.session_state.movimiento -= 1
with col3:
    jugada_input = st.number_input("Ir a jugada:", min_value=0, max_value=total,
                                   value=st.session_state.movimiento + 1, step=1,
                                   label_visibility="collapsed")
    st.session_state.movimiento = jugada_input - 1
with col4:
    if st.button("➡️"):
        if st.session_state.movimiento < total - 1:
            st.session_state.movimiento += 1
with col5:
    if st.button("⏭️"):
        st.session_state.movimiento = total - 1

# --- Dibujar tablero ---
board = chess.Board()
for i in range(st.session_state.movimiento + 1):
    try:
        board.push_san(jugadas[i])
    except Exception as e:
        st.error(f"Error en la jugada {i + 1}: {jugadas[i]} — {e}")
        break

orientacion = chess.WHITE if st.session_state.color_estudio == "W" else chess.BLACK
try:
    svg = chess.svg.board(board=board, size=400, orientation=orientacion)
    png = cairosvg.svg2png(bytestring=svg.encode('utf-8'))
except Exception as e:
    st.error(f"No se pudo generar el tablero: {e}")
    png = None

# --- Visualización ---
col_tablero, col_jugadas = st.columns([2, 1])
with col_tablero:
    st.subheader("Tablero")
    if png:
        st.image(png)
    else:
        st.warning("No se pudo mostrar el tablero.")

with col_jugadas:
    st.subheader("Jugadas")
    jugadas_mostradas = []
    actual = st.session_state.movimiento
    for i in range(0, len(jugadas), 2):
        num = (i // 2) + 1
        blanca = jugadas[i]
        negra = jugadas[i + 1] if i + 1 < len(jugadas) else ""
        if actual == i:
            texto = f"{num}. **{blanca}** {negra}"
        elif actual == i + 1:
            texto = f"{num}. {blanca} **{negra}**"
        else:
            texto = f"{num}. {blanca} {negra}"
        jugadas_mostradas.append(texto)

    st.markdown("\n".join(jugadas_mostradas))
