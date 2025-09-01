from fuzzy_utils import execute_dynamic_matching, display_results

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