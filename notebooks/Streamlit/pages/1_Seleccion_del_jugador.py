import streamlit as st
from pyspark.sql.functions import col

# Verificar que el DataFrame ya esté cargado en memoria
if 'df_partidas' not in st.session_state:
    st.error("Primero debes visitar la página principal para cargar los datos.")
    st.stop()

df = st.session_state.df_partidas

# Estado de jugador activo
if 'jugador_activo' not in st.session_state:
    st.session_state.jugador_activo = None
if 'mostrar_estudio' not in st.session_state:
    st.session_state.mostrar_estudio = False

st.title("Selección del jugador")

# --- Campo de búsqueda predictiva ---
busqueda = st.text_input("Buscar jugador por nombre o Fide ID:")

# --- Filtrar coincidencias ---
if busqueda:
    coincidencias = df.filter(
        col("nombre_completo").ilike(f"%{busqueda}%")
    ).select("nombre_completo").distinct().limit(10)

    opciones = [row["nombre_completo"] for row in coincidencias.collect()]

    if opciones:
        seleccion = st.selectbox("Coincidencias encontradas:", opciones)

        if st.button("Estudiar este jugador"):
            st.session_state.jugador_activo = seleccion
            st.session_state.partida_idx = 0
            st.session_state.movimiento = 0
            st.session_state.color_estudio = None
            st.session_state.mostrar_estudio = True
            st.success(f"Jugador seleccionado: {seleccion}")
    else:
        st.info("No se encontraron coincidencias.")
else:
    st.info("Escribe un nombre o ID para buscar.")

# --- Mostrar y permitir eliminar selección actual ---
if st.session_state.jugador_activo:
    nombre_id = st.session_state.jugador_activo.split(" - ")
    if len(nombre_id) == 2:
        nombre, fide_id = nombre_id
        st.markdown(f"### Jugador actual: `{nombre}` &nbsp;&nbsp;&nbsp;Fide ID: `{fide_id}`")
    else:
        st.markdown(f"### Jugador actual: `{st.session_state.jugador_activo}`")

    if st.button("Eliminar jugador seleccionado"):
        st.session_state.jugador_activo = None
        st.session_state.mostrar_estudio = False
        st.success("Jugador deseleccionado.")



