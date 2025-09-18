import mysql.connector
from rapidfuzz import process, fuzz
import pandas as pd
import json
from datetime import datetime

def connect_to_mysql(host, database, user, password=""):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

def calculate_weighted_score(source_record, target_record, weights):

    total_weight = sum(weights.values())
    weighted_scores = []
    
    for field, weight in weights.items():
        if field in source_record and field in target_record:
            score = fuzz.ratio(str(source_record[field]), str(target_record[field]))
            weighted_scores.append((score * weight) / total_weight)
            
    return sum(weighted_scores) if weighted_scores else 0

def fuzzy_match(source_record, choices, weights, score_cutoff=0):
    """
    Realiza coincidencia difusa con pesos personalizados por campo.
    
    Args:
        source_record (dict): Registro a comparar
        choices (list): Lista de registros objetivo para comparar
        weights (dict): Diccionario de pesos por columna (ej: {'nombre': 2, 'apellido': 3, 'email': 5})
        score_cutoff (float): Puntuación mínima para considerar una coincidencia
    
    Returns:
        dict: Mejor coincidencia encontrada con su puntuación
    """
    best_match = None
    best_score = score_cutoff
    
    for choice in choices:
        current_score = calculate_weighted_score(source_record, choice, weights)
        
        if current_score > best_score:
            best_score = current_score
            match_values = {
                'first_name': choice.get('first_name', choice.get('nombre')),
                'last_name': choice.get('last_name', choice.get('apellido')),
                'email': choice.get('email', '')
            }
            best_match = {
                'match': choice,
                'score': current_score,
                'match_result': f"{match_values['first_name']}{match_values['last_name']}",
                'match_result_values': json.dumps(match_values)
            }
    
    return best_match

def execute_dynamic_matching(params_dict, score_cutoff=0):
    """
    Ejecuta la coincidencia dinámica entre tablas usando pesos personalizados.
    
    Args:
        params_dict (dict): Diccionario con parámetros de conexión y mapeo
        score_cutoff (float): Puntuación mínima para considerar una coincidencia
    
    Returns:
        list: Lista de coincidencias encontradas
    """
    weights = {
        'nombre': 2,  # Peso para el nombre
        'apellido': 3,  # Peso para el apellido
        'email': 5,    # Peso para el email
    }
    
    conn = connect_to_mysql(
        host=params_dict.get("host", "localhost"),
        database=params_dict.get("database", ""),
        user=params_dict.get("user", "root"),
        password=params_dict.get("password", "")
    )
    cursor = conn.cursor(dictionary=True)

    # Consulta para obtener registros fuente
    src_fields = list(params_dict['src_dest_mappings'].keys())
    src_query = f"SELECT {', '.join(src_fields)} FROM {params_dict['sourceTable']}"
    cursor.execute(src_query)
    source_records = cursor.fetchall()

    # Consulta para obtener registros destino
    dest_fields = list(params_dict['src_dest_mappings'].values())
    dest_query = f"SELECT {', '.join(dest_fields)} FROM {params_dict['destTable']}"
    cursor.execute(dest_query)
    target_records = cursor.fetchall()

    results = []
    for src_record in source_records:
        match = fuzzy_match(
            source_record=src_record,
            choices=target_records,
            weights=weights,
            score_cutoff=score_cutoff
        )
        
        if match:
            results.append({
                'nombre': src_record.get('nombre'),
                'apellido': src_record.get('apellido'),
                'match_query': f"{src_record.get('nombre')}{src_record.get('apellido')}",
                'match_result': match['match_result'],
                'score': match['score'],
                'match_result_values': match['match_result_values'],
                'destTable': params_dict['destTable'],
                'sourceTable': params_dict['sourceTable'],
                'fecha_registro': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

    cursor.close()
    conn.close()
    return results

def display_results(resultados, output_format='dataframe'):
    """
    Muestra los resultados en el formato especificado.
    """
    if output_format == 'dataframe':
        df = pd.DataFrame(resultados)
        print("\nResultados en formato DataFrame:")
        print(df)
        return df
    else:
        print("\nResultados en formato diccionario:")
        for r in resultados:
            print(r)
        return resultados

def insertar_coincidentes_en_db(coincidentes, host="localhost", user="root", password="", database="crm"):
    """
    Inserta los registros coincidentes en la base de datos.
    """
    conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
    cursor = conn.cursor()
    
    for fila in coincidentes:
        cursor.execute("""
            CALL sp_InsertCoincidente(
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            fila['nombre'],
            fila['apellido'],
            fila['match_query'],
            fila['match_result'],
            fila['score'],
            fila['match_result_values'],
            fila['destTable'],
            fila['sourceTable']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"{len(coincidentes)} registros coincidentes insertados en la tabla Coincidentes.")

def separar_registros_coincidentes(resultados, umbral_score=70):
    """
    Separa los resultados en coincidentes y no coincidentes basado en el score.
    """
    coincidentes = [r for r in resultados if r['score'] >= umbral_score]
    no_coincidentes = [r for r in resultados if r['score'] < umbral_score]
    return coincidentes, no_coincidentes