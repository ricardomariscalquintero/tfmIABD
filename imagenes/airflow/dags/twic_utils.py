# dags/twic_utils.py

import os
import requests
import zipfile
import pandas as pd
import chess.pgn
from bs4 import BeautifulSoup
from hdfs import InsecureClient

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


# 2. Convertir todos los .pgn del directorio a un único CSV
def convertir_pgns_a_csv(dirPGNs="/tmp", csvPath="/tmp/partidas.csv", incluir_jugadas = False):
    partidas = []

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
                    ciudad = partes_site[0] if len(partes_site) == 2 else site
                    pais = partes_site[1] if len(partes_site) == 2 else ""

                    datos = {
                        "Event": headers.get("Event", ""),
                        "Site": headers.get("Site", ""),
                        "Ciudad": ciudad,
                        "Pais": pais,
                        "Date": headers.get("Date", ""),
                        "Round": headers.get("Round", ""),
                        "White": headers.get("White", ""),
                        "Black": headers.get("Black", ""),
                        "Result": headers.get("Result", ""),
                        "WhiteTitle": headers.get("WhiteTitle", ""),
                        "BlackTitle": headers.get("BlackTitle", ""),
                        "WhiteElo": headers.get("WhiteElo", ""),
                        "BlackElo": headers.get("BlackElo", ""),
                        "WhiteFideId": headers.get("WhiteFideId", ""),
                        "BlackFideId": headers.get("BlackFideId", ""),
                        "ECO": headers.get("ECO", ""),
                        "Opening": headers.get("Opening", ""),
                        "EventDate": headers.get("EventDate", "")
                    }

                    """if incluir_jugadas:
                        board = game.board()
                        jugadas = []
                        for move in game.mainline_moves():
                            jugadas.append(board.san(move))
                            board.push(move)
                        datos["Movimientos"] = " ".join(jugadas)"""

                    partidas.append(datos)

    df = pd.DataFrame(partidas)
    df.to_csv(csvPath, index=False)
    print(f"CSV generado correctamente: {csvPath}")


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

    # Limpieza de los archivos locales subidos
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
            print(f"⚠️ No se pudo eliminar {path} o su ZIP: {e}")



if __name__ == "__main__":
	descargar_pgns_local()
	convertir_pgns_a_csv()
	subir_a_hdfs()
    
