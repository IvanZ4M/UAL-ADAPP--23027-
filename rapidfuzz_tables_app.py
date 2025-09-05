import pandas as pd
import mysql.connector
import os
from fuzzy_utils import execute_dynamic_matching, display_results, export_results_to_csv, export_results_to_excel, separar_registros_coincidentes

def importar_csv_a_mysql(ruta_csv, params_dict):
    """
    Importa un archivo CSV y guarda los datos en la tabla 'clientes10'.
    Si la tabla no existe, se crea. Valida el archivo antes de importarlo.
    """
    nombre_tabla = "clientes10" 

    if not os.path.exists(ruta_csv):
        print(f"Error: El archivo '{ruta_csv}' no existe.")
        return
    
    try:
        df = pd.read_csv(ruta_csv)
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return

    columnas_requeridas = {'nombre', 'apellido', 'email'}
    if not columnas_requeridas.issubset(df.columns):
        print(f"Error: El archivo '{ruta_csv}' no tiene las columnas necesarias ({', '.join(columnas_requeridas)}).")
        return

    conn = mysql.connector.connect(
        host=params_dict["host"],
        user=params_dict["user"],
        password=params_dict["password"],
        database=params_dict["database"]
    )
    cursor = conn.cursor()

    sql_create = f"""
    CREATE TABLE IF NOT EXISTS `{nombre_tabla}` (
        cliente_id INT AUTO_INCREMENT PRIMARY KEY,
        nombre VARCHAR(100),
        apellido VARCHAR(100),
        email VARCHAR(150) UNIQUE,
        FechaRegistro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    cursor.execute(sql_create)

    for _, row in df.iterrows():
        nombre = row.get('nombre', None)
        apellido = row.get('apellido', None)
        email = row.get('email', None)

        sql_insert = f"""
        INSERT INTO `{nombre_tabla}` (nombre, apellido, email)
        VALUES (%s, %s, %s)
        """
        try:
            cursor.execute(sql_insert, (nombre, apellido, email))
        except mysql.connector.errors.IntegrityError as e:
            print(f"Registro ignorado: email duplicado o inválido (email: {email})")

    conn.commit()
    conn.close()
    print(f"Archivo '{ruta_csv}' importado exitosamente a la tabla '{nombre_tabla}'.")

print("¿Deseas importar un archivo CSV a la base de datos? (s/n): ", end="")
importar = input().strip().lower()
if importar == 's':
    ruta_csv = input("Escribe la ruta del archivo CSV a importar: ").strip()
    importar_csv_a_mysql(ruta_csv, {
        "host": "localhost",
        "database": "crm",
        "user": "root",
        "password": ""
    })
    print("Puedes continuar usando el sistema con la tabla 'clientes10' si lo deseas.\n")

params_dict = {
    "host": "localhost",
    "database": "crm",    
    "user": "root",
    "password": "",     
    "sourceTable": "Clientes",
    "destTable": "dbo.Usuarios", 
    "src_dest_mappings": {
        "nombre": "first_name",
        "apellido": "last_name"
    }
}

resultados = execute_dynamic_matching(params_dict, score_cutoff=70)
filtro = [r for r in resultados if r.get('score', 0) >= 70]

for fila in filtro:
    if 'score' in fila:
        fila['score'] = f"{round(float(fila['score']), 2)}%"

coincidentes, no_coincidentes = separar_registros_coincidentes(filtro)

formato = input("¿Cómo deseas mostrar los resultados? Escribe 'dataframe' o 'dict': ").strip().lower()
if formato not in ['dataframe', 'dict']:
    print("Formato no válido. Se usará 'dataframe' por defecto.")
    formato = 'dataframe'

display_results(coincidentes, output_format=formato)

if not coincidentes:
    print("No hay registros coincidentes para exportar. La exportación ha sido cancelada.")
else:
    exportar = input("¿Deseas exportar los registros coincidentes? (s/n): ").strip().lower()
    if exportar == 's':
        columnas_disponibles = list(coincidentes[0].keys())
        print("Columnas disponibles para exportar:")
        print(", ".join(columnas_disponibles))
        columnas_seleccionadas = input("Escribe las columnas que deseas exportar separadas por coma (ejemplo: nombre,apellido,score): ").strip()
        columnas_seleccionadas = [col.strip() for col in columnas_seleccionadas.split(",") if col.strip() in columnas_disponibles]

        if columnas_seleccionadas == ['score']:
            print("No puedes exportar solo la columna 'score'. Selecciona al menos una columna adicional.")
            exit()

        if not columnas_seleccionadas:
            print("No se seleccionaron columnas válidas. El sistema ha terminado y no se exportó ningún archivo.")
            exit()

        if 'score' not in columnas_seleccionadas and 'score' in columnas_disponibles:
            columnas_seleccionadas.append('score')

        crear_nombre_completo = False
        if 'nombre' in columnas_seleccionadas:
            crear_nombre_completo = True
            columnas_seleccionadas = [col for col in columnas_seleccionadas if col != 'nombre' and col != 'apellido']
            columnas_seleccionadas.insert(0, 'nombre_completo') 

        cambiar_nombres = input("¿Deseas cambiar el nombre de alguna columna seleccionada? (s/n): ").strip().lower()
        nombres_columnas = columnas_seleccionadas.copy()
        if cambiar_nombres == 's':
            for i, col in enumerate(columnas_seleccionadas):
                nuevo_nombre = input(f"Nuevo nombre para la columna '{col}' (deja vacío para mantener el nombre): ").strip()
                if nuevo_nombre:
                    nombres_columnas[i] = nuevo_nombre

        tipo_archivo = input("¿En qué formato deseas exportar? Escribe 'csv' o 'xlsx': ").strip().lower()
        nombre_archivo = input("Escribe el nombre del archivo (ejemplo: resultados.csv o resultados.xlsx): ").strip()
        if not nombre_archivo:
            nombre_archivo = f"resultados.{tipo_archivo}"
        limite = input(f"¿Cuántas filas deseas exportar? (máximo {len(coincidentes)}): ").strip()
        try:
            limite = int(limite)
            if limite < 1:
                print("No se puede exportar un archivo vacío o fuera de rango. La exportación ha sido cancelada.")
                datos_a_exportar = []
            else:
                datos_a_exportar = coincidentes[:limite]
        except ValueError:
            print(f"Valor no válido. Se exportarán todas las filas coincidentes ({len(coincidentes)}).")
            datos_a_exportar = coincidentes

        datos_a_exportar_final = []
        for fila in datos_a_exportar:
            nueva_fila = {}
            for i, col in enumerate(columnas_seleccionadas):
                if crear_nombre_completo and col == 'nombre_completo':
                    nombre = fila.get('nombre', '')
                    apellido = fila.get('apellido', '')
                    nueva_fila[nombres_columnas[i]] = f"{nombre} {apellido}".strip()
                else:
                    nueva_fila[nombres_columnas[i]] = fila.get(col, '')
            datos_a_exportar_final.append(nueva_fila)

        if not datos_a_exportar_final:
            print("No hay filas seleccionadas para exportar. La exportación ha sido cancelada.")
        elif tipo_archivo == 'csv':
            export_results_to_csv(datos_a_exportar_final, filename=nombre_archivo)
        elif tipo_archivo == 'xlsx':
            export_results_to_excel(datos_a_exportar_final, filename=nombre_archivo)
        else:
            print("Formato de exportación no válido. No se exportarán los resultados.")

if no_coincidentes:
    reparar = input("¿Deseas exportar los registros no coincidentes (score < 97%)? (s/n): ").strip().lower()
    if reparar == 's':
        nombre_archivo_reparar = input("Escribe el nombre del archivo para los registros no coincidentes (ejemplo: reparar.csv): ").strip()
        if not nombre_archivo_reparar:
            nombre_archivo_reparar = "registros_a_reparar.csv"
        export_results_to_csv(no_coincidentes, filename=nombre_archivo_reparar)
        print(f"Registros no coincidentes exportados a {nombre_archivo_reparar}.")