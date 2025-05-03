# dags/twic_utils.py

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
from iso3_paises import codigos_iso3_extras

# 1. Descargar los pgns en local.
def descargar_pgns_local(destLocal = "/tmp", maxArchivos = 10):
    
    # URL de TWIC
    url = "https://theweekinchess.com/twic"

    # Cabecera para evitar bloqueos del servidor
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
    }

    # Realizamos la petición a la web
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    # Buscamos todos los enlaces a ZIP PGN
    zipLinks = []  # Lista para almacenar los enlaces válidos

    # Recorremos todos los elementos <a> con atributo href
    for a in soup.find_all("a", href=True):
        href = a["href"]
        texto = a.text

        # Comprobamos si el enlace apunta a un archivo ZIP y menciona "PGN"
        if href.endswith(".zip") and "PGN" in texto:
            zipLinks.append(href)

    if not zipLinks:
        raise Exception("No se encontraron ZIPs PGN en la página.")

    # Si se indica máximo, acotar la lista
    if maxArchivos:
        zipLinks = zipLinks[:maxArchivos]

    # Procesamos cada ZIP
    for zipLink in zipLinks:
        if not zipLink.startswith("http"):
            continue

        filename = zipLink.split("/")[-1]
        pathLocal = os.path.join(destLocal, filename)

        print(f"Descargando: {zipLink}")
        try:
            r = requests.get(zipLink, headers=headers)
            with open(pathLocal, "wb") as f:
                f.write(r.content)
        except Exception as e:
            print(f"Error descargando {zipLink}: {e}")
            continue

        if not zipfile.is_zipfile(pathLocal):
            print(f"No es ZIP válido: {filename}")
            continue

        with zipfile.ZipFile(pathLocal, 'r') as zipRef:
            for archivo in zipRef.namelist():
                if archivo.endswith(".pgn"):
                    zipRef.extract(archivo, path=destLocal)
                    print(f"Extraído: {archivo}")

    print("Descarga y extracción completadas.")


# Geolocalizador con caché en memoria
geolocator = Nominatim(user_agent="twic_geo")
coordenadas_cache = {}

def obtener_coordenadas(ciudad, codigo_pais_iso3):
    ciudad_limpia = ciudad.strip().lower()
    if not ciudad_limpia or ciudad_limpia in {"?", "unknown", "n/a", "none"}:
        return None, None, iso3_a_nombre_pais(codigo_pais_iso3)

    clave = f"{ciudad}, {codigo_pais_iso3}"
    if clave in coordenadas_cache:
        return coordenadas_cache[clave]

    nombre_pais = iso3_a_nombre_pais(codigo_pais_iso3)

    try:
        location = geolocator.geocode(f"{ciudad}, {nombre_pais}", timeout=10)
        if location:
            latlon_nombre = (
                location.latitude,
                location.longitude,
                location.raw.get("address", {}).get("country", nombre_pais)
            )
            coordenadas_cache[clave] = latlon_nombre
            return latlon_nombre
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        print(f"⚠️ Error geolocalizando {clave}: {e}")
        time.sleep(2)
        return obtener_coordenadas(ciudad, codigo_pais_iso3)

    coordenadas_cache[clave] = (None, None, nombre_pais)
    return None, None, nombre_pais
    

def iso3_a_nombre_pais(iso3):
    iso3 = iso3.strip().upper()
    try:
        pais = pycountry.countries.get(alpha_3=iso3)
        if pais:
            return pais.name
    except Exception:
        pass
    # Fallback: buscar en los códigos adicionales
    return codigos_iso3_extras.get(iso3, "")


def normalizar_fecha(fecha_str):
    try:
        return datetime.strptime(fecha_str, "%Y.%m.%d").strftime("%Y-%m-%d")
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

def calcular_diferencia_elo(white_elo, black_elo):
    try:
        return int(white_elo) - int(black_elo)
    except:
        return 0

def clasificar_titulo_genero(titulo):
    titulo = str(titulo).strip()
    if not titulo:
        return "", False
    genero = "W" if titulo.startswith("W") else "M"
    return genero, True

def tipo_enfrentamiento(white_title, black_title):
    if white_title and black_title:
        return f"{white_title}-{black_title}"
    return ""
    
def nombre_titulo_abreviado(abreviatura):
    mapa_titulos = {
        "GM": "Gran Maestro",
        "IM": "Maestro Internacional",
        "FM": "Maestro FIDE",
        "CM": "Candidato a Maestro",
        "WGM": "Gran Maestra Femenina",
        "WIM": "Maestra Internacional Femenina",
        "WFM": "Maestra FIDE Femenina",
        "WCM": "Candidata a Maestra",
    }
    return mapa_titulos.get(str(abreviatura).strip().upper(), "")

# 2. Convertir todos los .pgn del directorio a un único CSV
def convertir_pgns_a_csv(dirPGNs="/tmp", csvPath="/tmp/partidas.csv", incluir_jugadas = False):
    campos = [
        "Event", "Site", "Ciudad", "PaisISO3", "Pais", "Latitud", "Longitud", "Date", "Año",
        "Round", "White", "Black", "Result", "Ganador", "ResultadoBinario",
        "WhiteTitle", "BlackTitle", "WhiteTituloNombre", "BlackTituloNombre",
        "WhiteHasTitle", "BlackHasTitle", "WhiteGenderTitle", "BlackGenderTitle",
        "WhiteElo", "BlackElo", "DiferenciaELO",
        "WhiteFideId", "BlackFideId", "ECO", "Opening", "EventDate", "MatchType"
    ]

    with open(csvPath, mode="w", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=campos)
        writer.writeheader()

        for archivo in os.listdir(dirPGNs):
            if archivo.endswith(".pgn"):
                ruta_pgn = os.path.join(dirPGNs, archivo)

                with open(ruta_pgn, encoding="utf-8", errors="ignore") as pgn_file:
                    while True:
                        game = chess.pgn.read_game(pgn_file)
                        if game is None:
                            break

                        headers = game.headers
                        site = headers.get("Site", "")
                        partes_site = site.rsplit(" ", 1)
                        ciudad = partes_site[0].strip() if len(partes_site) == 2 else site.strip()
                        pais_iso3 = partes_site[1].strip().upper() if len(partes_site) == 2 else ""
                        lat, lon, pais = obtener_coordenadas(ciudad, pais_iso3)

                        resultado = headers.get("Result", "")
                        ganador, resultado_bin = determinar_ganador(resultado)

                        white_elo = headers.get("WhiteElo", "")
                        black_elo = headers.get("BlackElo", "")
                        diff_elo = calcular_diferencia_elo(white_elo, black_elo)

                        white_title = headers.get("WhiteTitle", "")
                        black_title = headers.get("BlackTitle", "")

                        white_genero, white_has_title = clasificar_titulo_genero(white_title)
                        black_genero, black_has_title = clasificar_titulo_genero(black_title)

                        white_titulo_nombre = nombre_titulo_abreviado(white_title)
                        black_titulo_nombre = nombre_titulo_abreviado(black_title)

                        tipo = tipo_enfrentamiento(white_title, black_title)

                        fila = {
                            "Event": headers.get("Event", ""),
                            "Site": headers.get("Site", ""),
                            "Ciudad": ciudad,
                            "PaisISO3": pais_iso3,
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
                            "ResultadoBinario": resultado_bin,
                            "WhiteTitle": white_title,
                            "BlackTitle": black_title,
                            "WhiteTituloNombre": white_titulo_nombre,
                            "BlackTituloNombre": black_titulo_nombre,
                            "WhiteHasTitle": white_has_title,
                            "BlackHasTitle": black_has_title,
                            "WhiteGenderTitle": white_genero,
                            "BlackGenderTitle": black_genero,
                            "WhiteElo": white_elo,
                            "BlackElo": black_elo,
                            "DiferenciaELO": diff_elo,
                            "WhiteFideId": headers.get("WhiteFideId", ""),
                            "BlackFideId": headers.get("BlackFideId", ""),
                            "ECO": headers.get("ECO", ""),
                            "Opening": headers.get("Opening", ""),
                            "EventDate": normalizar_fecha(headers.get("EventDate", "")),
                            "MatchType": tipo
                        }

                        # Puedes incluir jugadas aquí si lo deseas
                        # if incluir_jugadas:
                        #     ...

                        writer.writerow(fila)
    
    print(f"✅ CSV generado línea a línea en: {csvPath}")


def subir_a_hdfs(dirLocal="/tmp", csvName="partidas.csv", hdfsURL="http://namenode:9870", hdfsPathPGN="/user/ajedrez/raw", hdfsPathCSV="/user/ajedrez/procesado"):

    client = InsecureClient(hdfsURL, user='root')

    # Crear carpetas si no existen
    client.makedirs(hdfsPathPGN)
    client.makedirs(hdfsPathCSV)

    archivos_subidos = []

    for archivo in os.listdir(dirLocal):
        localPath = os.path.join(dirLocal, archivo)

        if not os.path.exists(localPath):
            continue

        if archivo.endswith(".pgn"):
            hdfsDestino = os.path.join(hdfsPathPGN, archivo)
        elif archivo == csvName:
            hdfsDestino = os.path.join(hdfsPathCSV, archivo)
        else:
            continue

        try:
            print(f"📤 Subiendo: {archivo} → {hdfsDestino}")
            client.upload(hdfs_path=hdfsDestino, local_path=localPath, overwrite=True)
            archivos_subidos.append(localPath)
        except Exception as e:
            print(f"❌ Error subiendo {archivo}: {e}")
            continue

    print("✅ Subida a HDFS completada.")

    """# Limpieza de los archivos locales subidos
    for path in archivos_subidos:
        try:
            os.remove(path)
            print(f"🧹 Eliminado local: {path}")

            # Si es un .pgn, también eliminar el .zip asociado
            if path.endswith(".pgn"):
                base = os.path.splitext(path)[0]
                zip_path = base + ".zip"
                if os.path.exists(zip_path):
                    os.remove(zip_path)
                    print(f"🧹 Eliminado ZIP asociado: {zip_path}")
        except Exception as e:
            print(f"⚠️ No se pudo eliminar {path} o su ZIP: {e}")"""



"""if __name__ == "__main__":
	descargar_pgns_local()
	convertir_pgns_a_csv()
	subir_a_hdfs()"""
    
