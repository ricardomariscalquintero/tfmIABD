# dags/twic_utils.py

"""
Este script de python contiene las funciones auxiliares
que serán utilizadas desde el dag "desc_inic_pgn_warehouse"
que hemos creado para usarlo en Apache Airflow
"""

#Importamos las librerías.
import os
import requests
import zipfile
import pandas as pd
import chess.pgn
import time
import pycountry
import csv
from bs4 import BeautifulSoup
from hdfs import InsecureClient
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from datetime import datetime
from iso3_paises import codigosIso3Extras

"""
Esta función se utilizará para descargar los archivos de partidas PGNs
a nuestra máquina local. Recibirá la ruta local para la descarga y el máximo
de archivos a descargar.
"""

def descargar_pgns_local(destLocal = "/tmp", maxArchivos = 10):
    
    #URL de TWIC.
    url = "https://theweekinchess.com/twic"

    #Cabecera para evitar bloqueos del servidor.
    #Si no la usamos el servidor podrá bloquear nuestra petición por parecer un bot.
    cabeceras = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
    }

    #Realizamos la petición a la página y parseamos el contenido con BeautifulSoup.
    respuesta   = requests.get(url, headers = cabeceras)
    webParseada = BeautifulSoup(respuesta.content, "html.parser")

    #Creamos una lista que contendrá todos los enlaces de descarga.
    enlacesDescargas = []

    #Recorremos todos los enlaces.
    for a in webParseada.find_all("a", href = True):
        href  = a["href"]
        texto = a.text

        #Comprobamos si el enlace es a un fichero zip y menciona "PGN".
        if href.endswith(".zip") and "PGN" in texto:
            enlacesDescargas.append(href)

    if not enlacesDescargas:
        raise Exception("No se encontraron archivos de partidas para descargar en la página.")

    #Si el usuario indica un máximo de descargas, acotamos la lista.
    if maxArchivos:
        enlacesDescargas = enlacesDescargas[:maxArchivos]

    #Descargamos los ficheros ZIPs.
    for enlaceDescarga in enlacesDescargas:
        
        #Si no es un enlace, pasamos al siguiente.
        if not enlaceDescarga.startswith("http"):
            continue

        nombreFichero = enlaceDescarga.split("/")[-1]
        pathFichero   = os.path.join(destLocal, nombreFichero)

        print(f"Descargando: {enlaceDescarga}")
        
        #Descargamos el archivo con el enlace.
        try:
            r = requests.get(enlaceDescarga, headers = cabeceras)
            with open(pathFichero, "wb") as f:
                f.write(r.content)
        except Exception as e:
            print(f"Error descargando {enlaceDescarga}: {e}")
            continue

	#Si no es un fichero zip válido, pasamos al siguiente.
        if not zipfile.is_zipfile(pathFichero):
            print(f"No es ZIP válido: {nombreFichero}")
            continue

	#Extraemos los archivos PGNs.
	#Abrimos los Zips válidos, recorremos los archivos de su interior 
	#y extraemos los PGNS al directorio de destino.
        with zipfile.ZipFile(pathFichero, 'r') as zipRef:
            for archivo in zipRef.namelist():
                if archivo.endswith(".pgn"):
                    zipRef.extract(archivo, path=destLocal)
                    print(f"Extraído: {archivo}")

    print("Descarga y extracción completadas.")


######################################################################################################
# 		    Funciones auxiliares para el enriquecimiento del dataset.                        #
######################################################################################################

"""
Definimos un geolocalizador a partir de la clase Nominatum.
Nominatum, perteneciente a geopy, que se conecta al servicio OpenStreetMap,
nos permite convertir nombres de lugares en coordenadas geográficas y viceversa.
"""

geolocalizador = Nominatim(user_agent="twicGeo")
cacheCoordenadas = {}

"""
Esta función nos devolverá el nombre de un país a partir
de su código ISO 3. De no encontrarlo con la librería
pycountry, utilizaría el diccionario auxiliar.
"""

def iso3_a_nombre_pais(iso3):

    #Eliminamos los espacios al inicio y al final.
    #Además, lo pasamos a mayúsculas para normalizar la entrada.
    iso3 = iso3.strip().upper()
    
    #Buscamos el país a partir del código.
    try:
        pais = pycountry.countries.get(alpha_3 = iso3)
        
        if pais:
            return pais.name
            
    except Exception:
        pass #Si hay alguna excepción, continúa.
        
    #Si no ha encontrado el nombre del país, utiliza el diccionario auxiliar.
    #Si tampoco lo encuentra en el diccionario, devuelve vacío.
    return codigosIso3Extras.get(iso3, "")

"""
Esta función nos devolverá la latitud y longitud
asociadas a una determinada ciudad dentro de un país.
La función recibe una ciudad y código de país.
"""

def obtener_coordenadas(ciudad, codigoPaisIso3):

    #Normalizamos el nombre de la ciudad.
    ciudadLimpia = ciudad.strip().lower()
    
    #Comprobamos que sean nombres válidos de ciudades.
    #Si no es un nombre válido, devolvemos None como geocoordenadas.
    if not ciudadLimpia or ciudadLimpia in {"?", "unknown", "n/a", "none"}:
        return None, None, iso3_a_nombre_pais(codigoPaisIso3)

    #Con la seguridad que la ciudad está correcta, creamos una clave
    #y buscamos en la caché por si ya existe.
    clave = f"{ciudad}, {codigoPaisIso3}"
    
    if clave in cacheCoordenadas:
        return cacheCoordenadas[clave]

    #Buscamos el nombre del país.
    nombrePais = iso3_a_nombre_pais(codigoPaisIso3)

    try:
    	#Buscamos, a partir de la ciudad y el país, los datos
    	#geográficos asociados.
        lugar = geolocalizador.geocode(f"{ciudad}, {nombrePais}", timeout=10)
        
        #Si obtenemos una respuesta, generamos la estructura de datos
        #y la almacenamos en la cache.
        if lugar:
            latLonNombre = (
                lugar.latitude,
                lugar.longitude,
                lugar.raw.get("address", {}).get("country", nombrePais)
            )
            
            cacheCoordenadas[clave] = latLonNombre
            
            return latLonNombre
            
    #Si existe algún tipo de problema con la API,
    #esperamos 2 segundos y volvemos a repetir el proceso.
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        print(f"Error geolocalizando {clave}: {e}")
        
        time.sleep(2)
        
        return obtener_coordenadas(ciudad, codigoPaisIso3)


    #Si persiste el problema, devolvemos solamente el nombre
    #del país.
    cacheCoordenadas[clave] = (None, None, nombrePais)
    
    return None, None, nombrePais
    
"""
Las siguientes funciones son bastante escuetas y autoexplicativas.
"""

def normalizar_fecha(fecha):
    try:
        return datetime.strptime(fecha, "%Y.%m.%d").strftime("%Y-%m-%d")
    except Exception:
        return ""

def determinar_ganador(resultado):
    if resultado == "1-0":
        return "White", 1
    elif resultado == "0-1":
        return "Black", 0
    elif resultado == "1/2-1/2":
        return "Draw", 0.5
    return "", ""

def calcular_diferencia_elo(ELOBlancas, ELONegras):
    try:
        return int(ELOBlancas) - int(ELONegras)
    except:
        return 0

def clasificar_titulo_genero(titulo):
    titulo = str(titulo).strip()
    if not titulo:
        return "", False
    genero = "W" if titulo.startswith("W") else "M"
    return genero, True

def tipo_enfrentamiento(tituloBlancas, tituloNegras):
    if tituloBlancas and tituloNegras:
        return f"{tituloBlancas}-{tituloNegras}"
    return ""
    
def nombre_titulo_abreviado(abreviatura):
    mapaTitulos = {
        "GM": "Gran Maestro",
        "IM": "Maestro Internacional",
        "FM": "Maestro FIDE",
        "CM": "Candidato a Maestro",
        "WGM": "Gran Maestra Femenina",
        "WIM": "Maestra Internacional Femenina",
        "WFM": "Maestra FIDE Femenina",
        "WCM": "Candidata a Maestra",
    }
    return mapaTitulos.get(str(abreviatura).strip().upper(), "")

"""
Esta función convertirá todo el contenido de los ficheros PGNs en
un fichero CSV. Además, añadiremos datos a partir de conclusiones
extraídas tras realizar un análisis exploratorio de los diversos ficheros.
Para poder realizar el enriquecimiento nos hemos valido de los anteriores
métodos.
"""

def convertir_pgns_a_csv(dirPGNs="/tmp", csvPath="/tmp/partidas.csv"):

    #Definimos los campos del CSV resultante. Mantendremos algunos nombres inglés
    #para respetar la nomenclatura habitual en los archivos PGNs y los atributos
    #de la librería python-chess.
    campos = [
        "Event", "Site", "Ciudad", "PaisISO3", "Pais", "Latitud", "Longitud", "Date", "Año",
        "Round", "White", "Black", "Result", "Ganador", "ResultadoBinario",
        "WhiteTitle", "BlackTitle", "WhiteTituloNombre", "BlackTituloNombre",
        "WhiteHasTitle", "BlackHasTitle", "WhiteGenderTitle", "BlackGenderTitle",
        "WhiteElo", "BlackElo", "DiferenciaELO",
        "WhiteFideId", "BlackFideId", "ECO", "Opening", "EventDate", "MatchType"
    ]

    #Abrimos el fichero CSV resultante para empezar a escribir.
    with open(csvPath, mode = "w", newline = '', encoding = "utf-8") as ficheroCSV:
        writer = csv.DictWriter(ficheroCSV, fieldnames = campos)
        writer.writeheader()

        #A partir de la ruta de los PGNs, los recorremos y
        #vamos escribiendo en el CSV.
        for archivo in os.listdir(dirPGNs):
        
            #Si el fichero es un pgn realizamos el proceso.
            if archivo.endswith(".pgn"):
                rutaPgn = os.path.join(dirPGNs, archivo)

                with open(rutaPgn, encoding = "utf-8", errors = "ignore") as ficheroPGN:
                    while True:
                        #Cargamos la partida. 
                        #Si el proceso falla pasamos al siguiente fichero.
                        game = chess.pgn.read_game(ficheroPGN)
                        if game is None:
                            break

			#Realizamos los cálculos para obtener todos los valores que necesitamos.
                        headers        = game.headers
                        site           = headers.get("Site", "")
                        partesSite     = site.rsplit(" ", 1)
                        ciudad         = partesSite[0].strip() if len(partesSite) == 2 else site.strip()
                        paisIso3       = partesSite[1].strip().upper() if len(partesSite) == 2 else ""
                        lat, lon, pais = obtener_coordenadas(ciudad, paisIso3)

                        resultado             = headers.get("Result", "")
                        ganador, resultadoBin = determinar_ganador(resultado)

                        whiteElo = headers.get("WhiteElo", "")
                        blackElo = headers.get("BlackElo", "")
                        diffElo  = calcular_diferencia_elo(whiteElo, blackElo)

                        whiteTitle = headers.get("WhiteTitle", "")
                        blackTitle = headers.get("BlackTitle", "")

                        whiteGenero, whiteHasTitle = clasificar_titulo_genero(whiteTitle)
                        blackGenero, blackHasTitle = clasificar_titulo_genero(blackTitle)

                        whiteTituloNombre = nombre_titulo_abreviado(whiteTitle)
                        blackTituloNombre = nombre_titulo_abreviado(blackTitle)

                        tipo = tipo_enfrentamiento(whiteTitle, blackTitle)

                        #Con todos los datos calculados, procedemos a definir la fila.
                        fila = {
                            "Event": headers.get("Event", ""),
                            "Site": headers.get("Site", ""),
                            "Ciudad": ciudad,
                            "PaisISO3": paisIso3,
                            "Pais": pais,
                            "Latitud": lat,
                            "Longitud": lon,
                            "Date": normalizar_fecha(headers.get("Date", "")),
                            "Año": normalizar_fecha(headers.get("Date", ""))[:4],
                            "Round": headers.get("Round", ""),
                            "White": headers.get("White", ""),
                            "Black": headers.get("Black", ""),
                            "Result": resultado,
                            "Ganador": ganador,
                            "ResultadoBinario": resultadoBin,
                            "WhiteTitle": whiteTitle,
                            "BlackTitle": blackTitle,
                            "WhiteTituloNombre": whiteTituloNombre,
                            "BlackTituloNombre": blackTituloNombre,
                            "WhiteHasTitle": whiteHasTitle,
                            "BlackHasTitle": blackHasTitle,
                            "WhiteGenderTitle": whiteGenero,
                            "BlackGenderTitle": blackGenero,
                            "WhiteElo": whiteElo,
                            "BlackElo": blackElo,
                            "DiferenciaELO": diffElo,
                            "WhiteFideId": headers.get("WhiteFideId", ""),
                            "BlackFideId": headers.get("BlackFideId", ""),
                            "ECO": headers.get("ECO", ""),
                            "Opening": headers.get("Opening", ""),
                            "EventDate": normalizar_fecha(headers.get("EventDate", "")),
                            "MatchType": tipo
                        }

                        #Escribimos la fila en el CSV.
                        writer.writerow(fila)
    
    print(f"CSV generado línea a línea en: {csvPath}")

"""
Esta función subirá a HDFS todos los archivos PGNs,
además del CSV resultante.
"""

def subir_a_hdfs(dirLocal="/tmp", csvName="partidas.csv", hdfsURL="http://namenode:9870", hdfsPathPGN="/user/ajedrez/raw", hdfsPathCSV="/user/ajedrez/procesado"):

    #Iniciamos la conexión con el cliente.
    client = InsecureClient(hdfsURL, user='root')

    #Creamos los directorios si no existen.
    client.makedirs(hdfsPathPGN)
    client.makedirs(hdfsPathCSV)

    #Creamos una lista para registrar los archivos que se han
    #subido correctamente.
    archivosSubidos = []

    #Recorremos el directorio local y vamos procesando los archivos.
    for archivo in os.listdir(dirLocal):
        localPath = os.path.join(dirLocal, archivo)

        #Comprobamos que exista la ruta local.
        if not os.path.exists(localPath):
            continue

        #Comprobamos si es un archivo pgn o csv.
        #Esto lo hacemos ya que cada tipo va en un directorio distinto.
        if archivo.endswith(".pgn"):
            hdfsDestino = os.path.join(hdfsPathPGN, archivo)
        elif archivo == csvName:
            hdfsDestino = os.path.join(hdfsPathCSV, archivo)
        else:
            continue

        try:
            #Realizamos la subida del archivo y lo registramos en la lista de archivos subidos.
            print(f"Subiendo: {archivo} → {hdfsDestino}")
            client.upload(hdfs_path = hdfsDestino, local_path = localPath, overwrite = True)
            archivosSubidos.append(localPath)
        except Exception as e:
            print(f"Error subiendo {archivo}: {e}")
            continue

    print("Subida a HDFS completada.")

    
