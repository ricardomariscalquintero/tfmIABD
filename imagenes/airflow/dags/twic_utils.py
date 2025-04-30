# dags/twic_utils.py

import requests
from bs4 import BeautifulSoup
import os
from hdfs import InsecureClient
import zipfile

def descargar_ultimo_pgn(destLocal="/tmp", hdfsURL="http://namenode:9870", hdfsPath="/user/ajedrez/raw"):
    #URL de descarga
    url = "https://theweekinchess.com/twic"

    #Necesitamos la cabecera para simular la petición desde el navegador.
    #El servidor tiene capado el acceso vía requests.
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers)

    #Instanciamos un objeto soup con la respuesta del servidor.
    soup = BeautifulSoup(response.content, "html.parser")

    #Buscamos el enlace del último pgn.
    zipLink = None
    for a in soup.find_all("a", href = True):
        if a["href"].endswith(".zip") and "PGN" in a.text:
            zipLink = a["href"]
            break

    #Comprobamos que contenga el enlace.
    if not zipLink:
        raise Exception("No se encontró ningún ZIP PGN en la página.")

    #Comprobamos el protocolo del enlace.
    if not zipLink.startswith("http"):
        raise Exception("No es una URL válida.")

    filename = zipLink.split("/")[-1]
    pathLocal = os.path.join(destLocal, filename)
    hdfsDestino = os.path.join(hdfsPath, filename)

    print(f"El archivo {filename} se almacenará localmente en: {pathLocal}")

    #Paso 2: Descargaremos el archivo zip.
    print(f"Descargando: {zipLink}")
    r = requests.get(zipLink, headers=headers)

    with open(pathLocal, "wb") as f:
        f.write(r.content)
  
    print(f"Guardado localmente en: {pathLocal}")

    #Comprobamos si es un ZIP válido
    if not zipfile.is_zipfile(pathLocal):
        raise Exception("El archivo descargado {filename} no es un ZIP válido.")

    #Paso 3: Descomprimos el archivo.
    with zipfile.ZipFile(pathLocal, 'r') as zipRef:
        # Suponemos que solo hay un archivo dentro
        archivoPgn = zipRef.namelist()[0]
    
        if not archivoPgn.endswith(".pgn"):
            raise Exception(f"El archivo del ZIP no es un .pgn: {archivoPgn}")
    
        zipRef.extract(archivoPgn, path = destLocal)

    # Paso 4: Subimos el archivo PGN a HDFS
    print(f"Subiendo a HDFS en: {os.path.join(hdfsPath, archivoPgn)}")
    client = InsecureClient(hdfsURL, user='root')  # Cambia 'root' si usas otro usuario
    client.makedirs(hdfsPath)
    client.upload(os.path.join(hdfsPath, archivoPgn), os.path.join(destLocal, archivoPgn), overwrite=True)

    print("Subida a HDFS completada.")
    return 0
    

#if __name__ == "__main__":
#    descargar_ultimo_pgn()
    
