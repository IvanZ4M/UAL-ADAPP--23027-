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

        if not datos_a_exportar:
            print("No hay filas seleccionadas para exportar. La exportación ha sido cancelada.")
        elif tipo_archivo == 'csv':
            export_results_to_csv(datos_a_exportar, filename=nombre_archivo)
        elif tipo_archivo == 'xlsx':
            export_results_to_excel(datos_a_exportar, filename=nombre_archivo)
        else:
            print("Formato de exportación no válido. No se exportarán los resultados.")