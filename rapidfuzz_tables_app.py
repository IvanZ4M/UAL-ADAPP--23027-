from fuzzy_utils import execute_dynamic_matching, display_results, export_results_to_csv, export_results_to_excel

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

# Convertir score a porcentaje
for fila in filtro:
    if 'score' in fila:
        fila['score'] = f"{round(float(fila['score']), 2)}%"

formato = input("¿Cómo deseas mostrar los resultados? Escribe 'dataframe' o 'dict': ").strip().lower()
if formato not in ['dataframe', 'dict']:
    print("Formato no válido. Se usará 'dataframe' por defecto.")
    formato = 'dataframe'

display_results(filtro, output_format=formato)

if not filtro:
    print("No hay resultados para exportar. La exportación ha sido cancelada.")
else:
    exportar = input("¿Deseas exportar los resultados? (s/n): ").strip().lower()
    if exportar == 's':
        columnas_disponibles = list(filtro[0].keys())
        print("Columnas disponibles para exportar:")
        print(", ".join(columnas_disponibles))
        columnas_seleccionadas = input("Escribe las columnas que deseas exportar separadas por coma (ejemplo: nombre,apellido,score): ").strip()
        columnas_seleccionadas = [col.strip() for col in columnas_seleccionadas.split(",") if col.strip() in columnas_disponibles]

        if not columnas_seleccionadas:
            print("No se seleccionaron columnas válidas. El sistema ha terminado y no se exportó ningún archivo.")
            exit()

        # Agrega 'score' obligatoriamente si hay al menos una columna válida
        if 'score' not in columnas_seleccionadas and 'score' in columnas_disponibles:
            columnas_seleccionadas.append('score')

        # Si el usuario selecciona "nombre", crea columna nombre completo
        crear_nombre_completo = False
        if 'nombre' in columnas_seleccionadas:
            crear_nombre_completo = True
            # Elimina 'nombre' y 'apellido' si están en la selección
            columnas_seleccionadas = [col for col in columnas_seleccionadas if col != 'nombre' and col != 'apellido']
            columnas_seleccionadas.insert(0, 'nombre_completo')  # Puedes cambiar el orden si lo prefieres

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
        limite = input(f"¿Cuántas filas deseas exportar? (máximo {len(filtro)}): ").strip()
        try:
            limite = int(limite)
            if limite < 1 or limite > len(filtro):
                print("No se puede exportar un archivo vacío o fuera de rango. La exportación ha sido cancelada.")
                datos_a_exportar = []
            else:
                datos_a_exportar = filtro[:limite]
        except ValueError:
            print(f"Valor no válido. Se exportarán todas las filas ({len(filtro)}).")
            datos_a_exportar = filtro

        # Filtrar y renombrar columnas, y crear nombre completo si corresponde
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