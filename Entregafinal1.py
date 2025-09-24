import mysql.connector
import json
import os
from datetime import datetime
from config_loader import cargar_pesos_config
from fuzzy_utils import execute_dynamic_matching

def print_separador():
    print("\n" + "="*60 + "\n")

def asegurar_tablas(db_params, json_path):
    conn = mysql.connector.connect(**db_params)
    cursor = conn.cursor()
    # Tabla de pesos
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS configpesos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            columna VARCHAR(50) NOT NULL,
            peso FLOAT NOT NULL,
            fecha_actualizacion DATETIME NOT NULL
        )
    """)
    # Tabla de auditoría
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS auditoria_pesos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            columna VARCHAR(50) NOT NULL,
            peso_anterior FLOAT NOT NULL,
            peso_nuevo FLOAT NOT NULL,
            fecha_cambio DATETIME NOT NULL
        )
    """)
    # Inicializa pesos si la tabla está vacía
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
            if peso <= 0 or peso > 100:
                print(f"  ❌ Peso inválido para '{columna}'. Debe ser mayor a 0 y menor o igual a 100.")
                continue
            cursor.execute("SELECT peso FROM configpesos WHERE columna=%s", (columna,))
            result = cursor.fetchone()
            peso_anterior = result[0] if result else None
            if peso_anterior is not None and peso != peso_anterior:
                cursor.execute(
                    "UPDATE configpesos SET peso=%s, fecha_actualizacion=NOW() WHERE columna=%s",
                    (peso, columna)
                )
                cursor.execute(
                    "INSERT INTO auditoria_pesos (columna, peso_anterior, peso_nuevo, fecha_cambio) VALUES (%s, %s, %s, NOW())",
                    (columna, peso_anterior, peso)
                )
                print(f"✔ Peso de '{columna}' modificado de {peso_anterior} a {peso} y registrado en auditoría.")
            else:
                print(f"  No hubo cambio en el peso de '{columna}'.")
        except Exception as e:
            print(f"  Error para columna {columna}: {e}")
    conn.commit()
    cursor.close()
    conn.close()

def modificar_pesos_en_archivo(json_path):
    print_separador()
    print("ADMINISTRADOR: Modificar pesos en el archivo de configuración")
    columnas = ['nombre', 'apellido', 'email']
    nuevos_pesos = {}
    for columna in columnas:
        peso = input(f"Ingrese el nuevo peso para '{columna}': ")
        try:
            peso = float(peso)
            if peso <= 0 or peso > 100:
                print(f"  ❌ Peso inválido para '{columna}'. Debe ser mayor a 0 y menor o igual a 100.")
                return
            nuevos_pesos[columna] = peso
        except Exception as e:
            print(f"  Error para columna {columna}: {e}")
            return
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(nuevos_pesos, f, indent=4)
    print("✔ Pesos modificados en el archivo.")

def consultar_auditoria_pesos(db_params):
    print_separador()
    print("CONSULTA DE HISTORIAL DE MODIFICACIONES DE PESOS")
    columna = input("Filtrar por columna (deja vacío para ver todo): ").strip()
    orden = input("Ordenar por (columna/fecha): ").strip().lower()
    conn = mysql.connector.connect(**db_params)
    cursor = conn.cursor(dictionary=True)
    query = "SELECT columna, peso_anterior, peso_nuevo, fecha_cambio FROM auditoria_pesos"
    params = []
    if columna:
        query += " WHERE columna=%s"
        params.append(columna)
    if orden == "columna":
        query += " ORDER BY columna"
    elif orden == "fecha":
        query += " ORDER BY fecha_cambio DESC"
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    if rows:
        for row in rows:
            print(f"{row['fecha_cambio']} | {row['columna']} | {row['peso_anterior']} -> {row['peso_nuevo']}")
    else:
        print("No hay registros en el historial.")

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

def simular_impacto_pesos(db_params, json_path, params_dict):
    print_separador()
    print("SIMULACIÓN DE IMPACTO DE NUEVOS PESOS")
    columnas = ['nombre', 'apellido', 'email']
    nuevos_pesos = {}
    for columna in columnas:
        peso = input(f"Ingrese el peso SIMULADO para '{columna}': ")
        try:
            peso = float(peso)
            if peso <= 0 or peso > 100:
                print(f"  ❌ Peso inválido para '{columna}'. Debe ser mayor a 0 y menor o igual a 100.")
                return
            nuevos_pesos[columna] = peso
        except Exception as e:
            print(f"  Error para columna {columna}: {e}")
            return
    print("\nResultados de matching con pesos simulados:")
    resultados = execute_dynamic_matching(params_dict, score_cutoff=70, pesos=nuevos_pesos)
    for r in resultados[:5]:
        print(f"- Registro: {r}")
        print(f"  → Score simulado: {r.get('score', 'N/A')}\n")
    confirmar = input("¿Desea aplicar estos pesos? (s/n): ").strip().lower()
    if confirmar == "s":
        modificar_pesos_en_bd(db_params)
    else:
        print("No se aplicaron los cambios.")

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
        "sourceTable": "clientes_crm",
        "destTable": "dbo.Usuarios",
        "src_dest_mappings": {
            "nombre": "first_name",
            "apellido": "last_name",
            "email": "email"
        }
    }

    # Asegura que las tablas existan y los pesos por defecto estén cargados
    asegurar_tablas(db_params, json_path)

    print("\n" + "+" + "="*58 + "+")
    print("|{:^58}|".format("DEMO INTERACTIVA DE ADMINISTRACIÓN DE PESOS"))
    print("+" + "="*58 + "+\n")
    print("Opciones disponibles:")
    print("  1. Mostrar pesos actuales")
    print("  2. Modificar pesos en la base de datos (admin)")
    print("  3. Modificar pesos en el archivo de configuración (admin)")
    print("  4. Ejecutar matching como usuario final")
    print("  5. Prueba de sincronización y prevalencia")
    print("  6. Consultar historial de modificaciones de pesos")
    print("  7. Simular impacto de nuevos pesos antes de confirmar")
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
        elif opcion == "6":
            consultar_auditoria_pesos(db_params)
        elif opcion == "7":
            simular_impacto_pesos(db_params, json_path, params_dict)
        elif opcion == "0":
            print("\nSaliendo de la demo. ¡Gracias!\n")
            break
        else:
            print("Opción inválida. Intenta de nuevo.")

if __name__ == "__main__":
    menu()