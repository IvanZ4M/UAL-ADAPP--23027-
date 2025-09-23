import mysql.connector
import json
import os
from datetime import datetime
from config_loader import cargar_pesos_config
from fuzzy_utils import execute_dynamic_matching

def print_separador():
    print("\n" + "="*60 + "\n")

def asegurar_tabla_configpesos(db_params, json_path):
    conn = mysql.connector.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS configpesos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            columna VARCHAR(50) NOT NULL,
            peso FLOAT NOT NULL,
            fecha_actualizacion DATETIME NOT NULL
        )
    """)
    # Si la tabla está vacía, inicializa con los pesos del archivo JSON
    cursor.execute("SELECT COUNT(*) FROM configpesos")
    if cursor.fetchone()[0] == 0:
        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                pesos = json.load(f)
            datos = [(col, float(peso)) for col, peso in pesos.items()]
            cursor.executemany(
                "INSERT INTO configpesos (columna, peso, fecha_actualizacion) VALUES (%s, %s, NOW())",
                datos
            )
            conn.commit()
    cursor.close()
    conn.close()

def mostrar_pesos_actuales(db_params, json_path):
    print_separador()
    print("Pesos en la base de datos:")
    conn = mysql.connector.connect(**db_params)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT columna, peso, fecha_actualizacion FROM configpesos")
    for row in cursor.fetchall():
        print(f"  {row['columna']}: {row['peso']} (actualizado: {row['fecha_actualizacion']})")
    cursor.close()
    conn.close()
    print("\nPesos en el archivo de configuración:")
    if os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            pesos = json.load(f)
        print(f"  {pesos}")
        print(f"  (última modificación: {datetime.fromtimestamp(os.path.getmtime(json_path))})")
    else:
        print("  Archivo no encontrado.")

def modificar_pesos_en_bd(db_params):
    print_separador()
    print("ADMINISTRADOR: Modificar pesos en la base de datos")
    conn = mysql.connector.connect(**db_params)
    cursor = conn.cursor()
    columnas = ['nombre', 'apellido', 'email']
    for columna in columnas:
        peso = input(f"Ingrese el nuevo peso para '{columna}': ")
        try:
            peso = float(peso)
            cursor.execute(
                "UPDATE configpesos SET peso=%s, fecha_actualizacion=NOW() WHERE columna=%s",
                (peso, columna)
            )
        except Exception as e:
            print(f"  Error para columna {columna}: {e}")
    conn.commit()
    cursor.close()
    conn.close()
    print("✔ Pesos modificados en la base de datos.")

def modificar_pesos_en_archivo(json_path):
    print_separador()
    print("ADMINISTRADOR: Modificar pesos en el archivo de configuración")
    columnas = ['nombre', 'apellido', 'email']
    nuevos_pesos = {}
    for columna in columnas:
        peso = input(f"Ingrese el nuevo peso para '{columna}': ")
        try:
            nuevos_pesos[columna] = float(peso)
        except Exception as e:
            print(f"  Error para columna {columna}: {e}")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(nuevos_pesos, f, indent=4)
    print("✔ Pesos modificados en el archivo.")

def ejecutar_matching(db_params, json_path, params_dict):
    print_separador()
    print("USUARIO FINAL: Ejecutando matching (los pesos se sincronizan automáticamente)")
    pesos = cargar_pesos_config(json_path, db_params)
    print(f"Pesos utilizados por el sistema: {pesos}\n")
    resultados = execute_dynamic_matching(params_dict, score_cutoff=70, pesos=pesos)
    print("Resultados del matching (primeros 5 registros):")
    for r in resultados[:5]:
        print(f"- Registro: {r}")
        print(f"  → Score asignado (ponderado por pesos): {r.get('score', 'N/A')}\n")
    print("✔ Matching ejecutado.")

def prueba_sincronizacion(db_params, json_path, params_dict):
    print_separador()
    print("PRUEBA DE SINCRONIZACIÓN Y POLÍTICA DE PREVALENCIA")
    print("1. Modificando el archivo de configuración con fecha más reciente...")
    columnas = ['nombre', 'apellido', 'email']
    nuevos_pesos_archivo = {}
    for columna in columnas:
        nuevos_pesos_archivo[columna] = float(input(f"Peso para '{columna}' en archivo: "))
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(nuevos_pesos_archivo, f, indent=4)
    import time; time.sleep(1)
    print("2. Modificando la base de datos con fecha más reciente...")
    conn = mysql.connector.connect(**db_params)
    cursor = conn.cursor()
    for columna in columnas:
        peso = float(input(f"Peso para '{columna}' en BD: "))
        cursor.execute(
            "UPDATE configpesos SET peso=%s, fecha_actualizacion=NOW() WHERE columna=%s",
            (peso, columna)
        )
    conn.commit()
    cursor.close()
    conn.close()
    print("3. Ejecutando matching (debe usar los pesos de la BD)...")
    ejecutar_matching(db_params, json_path, params_dict)
    print("✔ Fin de la prueba de sincronización y prevalencia.")

def menu():
    db_params = {
        "host": "localhost",
        "user": "root",
        "password": "",
        "database": "crm"
    }
    json_path = "config_pesos.json"
    params_dict = {
        "host": "localhost",
        "database": "crm",
        "user": "root",
        "password": "",
        "sourceTable": "clientes10",
        "destTable": "dbo.Usuarios",
        "src_dest_mappings": {
            "nombre": "first_name",
            "apellido": "last_name",
            "email": "email"
        }
    }

    # Asegura que la tabla configpesos exista y tenga los pesos por defecto del archivo
    asegurar_tabla_configpesos(db_params, json_path)

    print("\n" + "+" + "="*58 + "+")
    print("|{:^58}|".format("DEMO INTERACTIVA DE ADMINISTRACIÓN DE PESOS"))
    print("+" + "="*58 + "+\n")
    print("Opciones disponibles:")
    print("  1. Mostrar pesos actuales")
    print("  2. Modificar pesos en la base de datos (admin)")
    print("  3. Modificar pesos en el archivo de configuración (admin)")
    print("  4. Ejecutar matching como usuario final")
    print("  5. Prueba de sincronización y prevalencia")
    print("  0. Salir\n")

    while True:
        opcion = input("Selecciona una opción: ")
        if opcion == "1":
            mostrar_pesos_actuales(db_params, json_path)
        elif opcion == "2":
            modificar_pesos_en_bd(db_params)
        elif opcion == "3":
            modificar_pesos_en_archivo(json_path)
        elif opcion == "4":
            ejecutar_matching(db_params, json_path, params_dict)
        elif opcion == "5":
            prueba_sincronizacion(db_params, json_path, params_dict)
        elif opcion == "0":
            print("\nSaliendo de la demo. ¡Gracias!\n")
            break
        else:
            print("Opción inválida. Intenta de nuevo.")

if __name__ == "__main__":
    menu()