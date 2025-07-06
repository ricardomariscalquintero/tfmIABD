#Importamos las librerías necesarias.
import pandas as pd
import chess.pgn
import os

#Definimos las rutas de entrada y la codificación.
csvPath  = "partidasDW.csv"
pgnDir   = "pgns/"
#encoding = "utf-8"
encoding = "latin1"

#Cargamos el CSV con las partidas del Data Warehouse
print(f"Leemos el fichero CSV desde: {csvPath}...")

#Cargamos el dataframe y parseamos la columna "fecha_partida" y la formateamos al estilo PGN.
#Además, convertimos el campo Round a cadena, eliminando la parte decimal ".0".
dfDW                  = pd.read_csv(csvPath, parse_dates = ["fecha_partida"])
dfDW["fecha_partida"] = dfDW["fecha_partida"].dt.strftime("%Y.%m.%d")
dfDW["Round"]         = dfDW["Round"].astype(str).str.rstrip(".0")

print(f"Fichero CSV cargado con {len(dfDW)} partidas.\n")

#Desarrollamos funciones auxiliares para adaptar los ficheros PGN y CSV.
def convertir_resultado(result):
    if result == "1-0":
        return 1.0, "White"
    elif result == "0-1":
        return 0.0, "Black"
    elif result == "1/2-1/2":
        return 0.5, "Draw"
    return None, None

def seguro_float(valor):
    try:
        if valor is None or pd.isna(valor):
            return "Sin ELO"
        return float(valor)
    except (ValueError, TypeError):
        return "Sin ELO"

#En el DW, cuando no se conoce la apertura, se introduce DESC y Desconocida.
#En los PGNs no está así, por eso necesitamos esta función.
def limpiar_texto_pgn(campo, valor):
    if campo == "ECO" and (valor is None or str(valor).strip() == ""):
        return "DESC"
    if campo == "Opening" and (valor is None or str(valor).strip() == ""):
        return "Desconocida"
    return str(valor).strip()

def normalizar_valor(valor):
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return "Sin título"
    return str(valor).strip()

#Desarrollamos esta función qué, además de comprobar si son dos campos iguales,
#nos permite considerar equivalentes None y NaN.
def iguales(valorPGN, valorDW):
    if (valorPGN is None or pd.isna(valorPGN)) and (valorDW is None or pd.isna(valorDW)):
        return True
        
    return str(valorPGN).strip() == str(valorDW).strip()

#Iniciamos el procesamiento de los ficheros PGNs
#Definimos las variables relicionadas con las estadísticas generales.
errores       = []
totalPartidas = 0
coinciden     = 0
faltan        = 0

print(f"Analizamos PGNs en el directorio: {pgnDir}\n")

#Calculamos el número total de partidas para indicar el progreso del análisis.
numTotalEstimado = 0

#Recorremos el directorio, cargando cada fichero y contabilizando el número de partidas.
for archivo in os.listdir(pgnDir):
    if archivo.lower().endswith(".pgn"):
        with open(os.path.join(pgnDir, archivo), encoding=encoding) as f:
            while chess.pgn.read_game(f):
                numTotalEstimado += 1

#Procesamos los ficheros PGNs y vamos realizando las comprobaciones.
for ficheroPGN in sorted(os.listdir(pgnDir)):

    #Si el fichero no tiene extensión pgn, pasamos al siguiente.
    if not ficheroPGN.lower().endswith(".pgn"):
        continue

    print(f"\nProcesando archivo: {ficheroPGN}")
    
    with open(os.path.join(pgnDir, ficheroPGN), encoding=encoding) as ficheroPGNTemp:
        while True:
            #Cargamos la partida y comprobamos.
            partida = chess.pgn.read_game(ficheroPGNTemp)
            if partida is None:
                break

            totalPartidas += 1
            print(f"\rProgreso global: {totalPartidas}/{numTotalEstimado}", end="")

            #Obtenemos los metadatos de la partida actual.
            h = partida.headers

            #Creamos una clave que nos sirva para buscar en el CSV del DW.
            clave = (
                str(h.get("WhiteFideId", "")).strip(),
                str(h.get("BlackFideId", "")).strip(),
                str(h.get("Date", "")).strip(),
                str(h.get("Round", "")).strip().rstrip(".0"),
                normalizar_valor(h.get("Event"))
            )

            #Filtramos en el dataframe por la clave generada.
            resultadoCsv = dfDW[
                (dfDW["fide_blanco"].astype(str) == clave[0]) &
                (dfDW["fide_negro"].astype(str) == clave[1]) &
                (dfDW["fecha_partida"] == clave[2]) &
                (dfDW["Round"] == clave[3]) &
                (dfDW["evento"] == clave[4])
            ]

            #Si no hay resultado, no hemos conseguido encontrar en el almacén de datos
            #la partida actual del PGN.
            if resultadoCsv.empty:
                errores.append(f"No encontrada en el DW: ID compuesto {clave} (fichero {ficheroPGN})")
                faltan += 1
                continue

            #Si encuentra la fila, accedemos a la misma y obtenemos el id.
            row = resultadoCsv.iloc[0]
            id_partida = row["id_partida"]
            
            #Transformamos los resultados.
            rBinario, rGanador = convertir_resultado(h.get("Result"))

            #Calculamos los nombres de los ganadores para cotejarlos.
            nombreGanadorPgn = h.get("White") if rGanador == "White" else h.get("Black") if rGanador == "Black" else None
            nombreGanadorDwh = row["jugador_blanco"] if rGanador == "White" else row["jugador_negro"] if rGanador == "Black" else None

            #Definimos la estructura para poder comparar los campos del PGN y CSV.
            campos = [
                ("jugador_blanco", normalizar_valor(h.get("White")), normalizar_valor(row["jugador_blanco"])),
                ("jugador_negro", normalizar_valor(h.get("Black")), normalizar_valor(row["jugador_negro"])),
                ("elo_blanco", seguro_float(h.get("WhiteElo")), seguro_float(row["elo_blanco"])),
                ("elo_negro", seguro_float(h.get("BlackElo")), seguro_float(row["elo_negro"])),
                ("titulo_blanco", normalizar_valor(h.get("WhiteTitle")), normalizar_valor(row["titulo_blanco"])),
                ("titulo_negro", normalizar_valor(h.get("BlackTitle")), normalizar_valor(row["titulo_negro"])),
                ("evento", normalizar_valor(h.get("Event")), normalizar_valor(row["evento"])),
                ("lugar", normalizar_valor(h.get("Site")), normalizar_valor(row["lugar"])),
                ("ECO", limpiar_texto_pgn("ECO", h.get("ECO")), normalizar_valor(row["ECO"])),
                ("Opening", limpiar_texto_pgn("Opening", h.get("Opening")), normalizar_valor(row["Opening"])),
                ("ResultadoBinario", rBinario, row["ResultadoBinario"]),
                ("Ganador", rGanador, row["Ganador"]),
                ("id_ganador", row["id_ganador"], row["id_ganador"]),
                ("nombre_ganador", normalizar_valor(nombreGanadorPgn), normalizar_valor(nombreGanadorDwh)),
            ]

            #Comparamos los campos.
            coincide = True
            for campo, valorPGN, valorDW in campos:
            
                #Si son distintos los valores, registramos la diferencia.
                if not iguales(valorPGN, valorDW):
                    errores.append(
                        f"Diferencia en id_partida = {id_partida} → {campo}: PGN = '{valorPGN}' vs DWH = '{valorDW}'"
                    )
                    
                    coincide = False

            if coincide:
                coinciden += 1

#Mostramos las estadísticas finales.
print("\n\n Resultado final:")
print(f"   Total partidas procesadas: {totalPartidas}")
print(f"   Coincidencias exactas:     {coinciden}")
print(f"   No encontradas en DWH:     {faltan}")
print(f"   Diferencias detectadas:    {len(errores)}\n")

#Si existen errores lo mostramos.
if errores:
    print("Se encontraron discrepancias:\n")
    for e in errores:
        print(e)
else:
    print("Todos los datos del DW coinciden con los PGN.")
    
