from fuzzy_utils import execute_dynamic_matching

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
print(filtro)

