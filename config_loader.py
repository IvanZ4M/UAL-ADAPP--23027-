import json
import mysql.connector
import os
from datetime import datetime

def cargar_pesos_desde_archivo(ruta):
    if not os.path.exists(ruta):
        return {}, None
    with open(ruta, 'r', encoding='utf-8') as f:
        pesos = json.load(f)
    fecha = datetime.fromtimestamp(os.path.getmtime(ruta))
    return pesos, fecha

def cargar_pesos_desde_db(conn):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT columna, peso, fecha_actualizacion FROM configpesos")
    pesos = {}
    fecha_max = None
    for row in cursor.fetchall():
        pesos[row['columna']] = float(row['peso'])
        if not fecha_max or row['fecha_actualizacion'] > fecha_max:
            fecha_max = row['fecha_actualizacion']
    cursor.close()
    return pesos, fecha_max

def actualizar_db_desde_archivo(conn, pesos_archivo):
    cursor = conn.cursor()
    for columna, peso in pesos_archivo.items():
        cursor.execute(
            "UPDATE configpesos SET peso=%s, fecha_actualizacion=NOW() WHERE columna=%s",
            (peso, columna)
        )
    conn.commit()
    cursor.close()

def actualizar_archivo_desde_db(ruta, pesos_db):
    with open(ruta, 'w', encoding='utf-8') as f:
        json.dump(pesos_db, f, indent=4)

def cargar_pesos_config(json_path, db_params):
    try:
        conn = mysql.connector.connect(
            host=db_params["host"],
            user=db_params["user"],
            password=db_params["password"],
            database=db_params["database"]
        )
        pesos_archivo, fecha_archivo = cargar_pesos_desde_archivo(json_path)
        pesos_db, fecha_db = cargar_pesos_desde_db(conn)

        # Si no hay pesos en DB, usar archivo y actualizar DB
        if not pesos_db and pesos_archivo:
            actualizar_db_desde_archivo(conn, pesos_archivo)
            conn.close()
            return pesos_archivo

        # Si no hay archivo, usar DB y actualizar archivo
        if not pesos_archivo and pesos_db:
            actualizar_archivo_desde_db(json_path, pesos_db)
            conn.close()
            return pesos_db

        # Si ambos existen, comparar fechas
        if fecha_db and fecha_archivo:
            if fecha_db > fecha_archivo:
                # Actualizar archivo desde DB
                actualizar_archivo_desde_db(json_path, pesos_db)
                conn.close()
                return pesos_db
            else:
                # Actualizar DB desde archivo
                actualizar_db_desde_archivo(conn, pesos_archivo)
                conn.close()
                return pesos_archivo
        else:
            # Si no hay fechas válidas, prioriza DB
            conn.close()
            return pesos_db if pesos_db else pesos_archivo
    except Exception as e:
        print(f"Error sincronizando pesos: {e}")
        # Si hay error, intenta cargar solo el archivo
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error leyendo el archivo de pesos: {e}")
            return {}