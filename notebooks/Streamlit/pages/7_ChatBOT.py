import streamlit as st
import pandas as pd
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_groq import ChatGroq

# Configuración de la página
st.set_page_config(page_title="ChatBOT", layout="wide")
st.title("ChatBOT")

# Verificar clave de API
groq_key = st.secrets.get("groq", {}).get("API_KEY", "")
if not groq_key:
    st.error("No se ha configurado la API KEY de Groq.")
    st.stop()

# Inicializar modelo
model = ChatGroq(
    model="llama3-70b-8192",
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
    api_key=groq_key,
)

# Estado de conversación
if "messages" not in st.session_state:
    st.session_state.messages = []

# Verificar que el DataFrame esté cargado
if "df_partidas" not in st.session_state:
    st.error("Primero debes cargar los datos en la página principal.")
    st.stop()

df = st.session_state.df_partidas.toPandas()

# Normalizar columna color
df["color"] = df["color"].astype(str).str.strip().str.lower()
df["color"] = df["color"].replace({
    "blanco": "blancas", "white": "blancas", "w": "blancas",
    "negro": "negras", "black": "negras", "b": "negras"
})

# Color seleccionado por el usuario
nombre_jugador, fide_id = st.session_state.jugador_activo.split(" - ")
st.subheader(f"Elija el color de las fichas con las que jugó {nombre_jugador}")

# Inicializar selección si no está definida
if "color_seleccionado" not in st.session_state:
    st.session_state.color_seleccionado = "blancas"

# Crear botones tipo toggle horizontales
col1, col2 = st.columns(2)
with col1:
    if st.button("Blancas", type="primary" if st.session_state.color_seleccionado == "blancas" else "secondary"):
        st.session_state.color_seleccionado = "blancas"

with col2:
    if st.button("Negras", type="primary" if st.session_state.color_seleccionado == "negras" else "secondary"):
        st.session_state.color_seleccionado = "negras"

# Asignar color filtrado a variable
color_filtrado = st.session_state.color_seleccionado


# Verificar jugador seleccionado
if "jugador_activo" not in st.session_state:
    st.warning("Primero selecciona un jugador en la sección anterior.")
    st.stop()

#st.markdown(f"Analizando partidas de **{nombre_jugador}** jugando con **{color_filtrado}**")

# Filtrar partidas por color y jugador
df_color = df[
    (df["color"] == color_filtrado) &
    (df["jugador"] == nombre_jugador) &
    (df["fide_id"].astype(str) == fide_id)
]

# Validación
if df_color.empty:
    st.warning(f"No se encontraron partidas de {nombre_jugador} como {color_filtrado}.")
    st.stop()

# Límite ajustable
limite = st.slider("Número máximo de partidas a analizar:", 10, 300, 100)
df_filtrado = df_color.sample(min(len(df_color), limite), random_state=42)

# Reducir columnas para evitar exceso de tokens
columnas_utiles = [
    "jugador", "color", "elo", "resultados", 
    "evaluacion_cp", "errores", "evento", "fechas"
]
df_filtrado = df_filtrado[[col for col in columnas_utiles if col in df_filtrado.columns]]

# Mostrar resumen
st.markdown(f"Se analizarán **{len(df_filtrado)}** partidas de `{nombre_jugador}` como **{color_filtrado}**.")

# Crear agente
agent = create_pandas_dataframe_agent(
    model,
    df_filtrado,
    verbose=False,
    handle_parsing_errors=True,
    allow_dangerous_code=True
)

# Chat
if prompt := st.chat_input("Haz una pregunta sobre las partidas filtradas..."):
    prompt_final = (
        f"Eres un experto en análisis de partidas de ajedrez. "
        f"Responde solo en español, de forma clara y directa. "
        f"Usa tablas Markdown si es útil.\n\nPregunta: {prompt}"
    )

    with st.chat_message("user"):
        st.markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt_final})

    with st.spinner("Procesando la consulta..."):
        try:
            response = agent.run(prompt_final)
        except Exception as e:
            # Si la excepción contiene una respuesta útil, extraerla del mensaje
            mensaje = str(e)
            if "Could not parse LLM output:" in mensaje:
                # Extraer solo la parte útil del mensaje
                respuesta_util = mensaje.split("Could not parse LLM output:")[-1].strip()
                response = respuesta_util
            else:
                response = f"Error al procesar la consulta: {e}"


    with st.chat_message("assistant"):
        st.markdown(response)

    st.session_state.messages.append({"role": "assistant", "content": response})
