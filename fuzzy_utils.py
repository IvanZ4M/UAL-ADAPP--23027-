def insertar_coincidentes_en_db(coincidentes, host="localhost", user="root", password="", database="crm"):
    import mysql.connector
    conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
    cursor = conn.cursor()
    for fila in coincidentes:
        score_val = fila.get('score', 0)
        if isinstance(score_val, str):
            score_val = score_val.replace('%', '').strip()
            try:
                score_val = float(score_val)
            except ValueError:
                score_val = 0.0
        def safe_get(key):
            return fila[key] if key in fila else None
        try:
            cursor.callproc('sp_InsertCoincidente', [
                safe_get('nombre'),
                safe_get('apellido'),
                safe_get('match_query'),
                safe_get('match_result'),
                score_val,
                safe_get('match_result_values'),
                safe_get('destTable'),
                safe_get('sourceTable')
            ])
        except Exception as e:
            print(f"Error insertando fila en Coincidentes: {e}\nFila: {fila}")
    conn.commit()
    cursor.close()
    conn.close()
    print(f"{len(coincidentes)} registros coincidentes insertados en la tabla Coincidentes de la base de datos crm.")

def mostrar_coincidentes_recientes(host="localhost", user="root", password="", database="crm", limite=20):
    import mysql.connector
    conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM Coincidentes ORDER BY fecha_insercion DESC LIMIT %s", (limite,))
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()
    import pandas as pd
    df = pd.DataFrame(resultados)
    print(df)
    return df

from rapidfuzz import process, fuzz
import mysql.connector
import pandas as pd
import os

def connect_to_mysql(host, database, user, password=""):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

def fuzzy_match(queryRecord, choices, score_cutoff=0, pesos=None):
    best_match = None
    best_score = score_cutoff

    for choice in choices:
        total_score = 0
        total_peso = 0
        campos_usados = []
        if pesos:
            for campo, peso in pesos.items():
                valor_query = str(queryRecord.get(campo, "")).lower()
                valor_choice = str(choice.get(campo, "")).lower()
                campo_score = fuzz.ratio(valor_query, valor_choice)
                total_score += campo_score * peso
                total_peso += peso
                campos_usados.append(campo)
            score_final = total_score / total_peso if total_peso else 0
        else:
            score_final = fuzz.ratio(str(queryRecord), " ".join([str(v) for v in choice.values()]))

        if score_final >= best_score:
            best_score = score_final
            best_match = {
                'match_query': " ".join([str(queryRecord.get(c, "")) for c in pesos.keys()]) if pesos else str(queryRecord),
                'match_result': " ".join([str(choice.get(c, "")) for c in pesos.keys()]) if pesos else " ".join([str(v) for v in choice.values()]),
                'score': score_final,
                'match_result_values': {k: choice.get(k, "") for k in pesos.keys()} if pesos else choice
            }
    # MODIFICACIÓN: Siempre retorna un diccionario, nunca None
    if best_match is not None:
        return best_match
    else:
        return {
            'match_query': "",
            'match_result': "",
            'score': 0,
            'match_result_values': {k: "" for k in pesos.keys()} if pesos else {},
        }

def execute_dynamic_matching(params_dict, score_cutoff=0, pesos=None):
    conn = connect_to_mysql(
        host=params_dict.get("host", "localhost"),
        database=params_dict.get("database", ""),
        user=params_dict.get("user", "root"),
        password=params_dict.get("password", "")
    )
    cursor = conn.cursor()

    if 'src_dest_mappings' not in params_dict or not params_dict['src_dest_mappings']:
        raise ValueError("Debe proporcionar src_dest_mappings con columnas origen y destino")

    src_cols = ", ".join(params_dict['src_dest_mappings'].keys())
    dest_cols = ", ".join(params_dict['src_dest_mappings'].values())

    sql_source = f"SELECT {src_cols} FROM {params_dict['sourceTable']}"
    sql_dest   = f"SELECT {dest_cols} FROM {params_dict['destTable']}"

    cursor.execute(sql_source)
    src_rows = cursor.fetchall()
    src_columns = [col[0] for col in cursor.description]
    source_data = [dict(zip(src_columns, row)) for row in src_rows]

    cursor.execute(sql_dest)
    dest_rows = cursor.fetchall()
    dest_columns = [col[0] for col in cursor.description]
    dest_data = [dict(zip(dest_columns, row)) for row in dest_rows]

    conn.close()

    matching_records = []

    for record in source_data:
        query_dict = {}
        for src_col, dest_col in params_dict['src_dest_mappings'].items():
            query_dict[src_col] = record.get(src_col)
        # Prepara los choices para que los nombres coincidan con los src_col
        choices_mapeados = []
        for dest_row in dest_data:
            choice_dict = {}
            for src_col, dest_col in params_dict['src_dest_mappings'].items():
                choice_dict[src_col] = dest_row.get(dest_col)
            choices_mapeados.append(choice_dict)
        fm = fuzzy_match(query_dict, choices_mapeados, score_cutoff, pesos)
        record.update(fm)
        record.update({
            'destTable': params_dict['destTable'],
            'sourceTable': params_dict['sourceTable']
        })
        matching_records.append(record)

    return matching_records

def display_results(resultados, output_format='dataframe'):
    if output_format == 'dataframe':
        df = pd.DataFrame(resultados)
        print(df)
        return df
    elif output_format == 'dict':
        print(resultados)
        return resultados
    else:
        raise ValueError("output_format debe ser 'dataframe' o 'dict'")

def export_results_to_csv(resultados, filename="resultados.csv"):
    carpeta = "exportados"
    os.makedirs(carpeta, exist_ok=True)
    ruta_completa = os.path.join(carpeta, filename)
    df = pd.DataFrame(resultados)
    df.to_csv(ruta_completa, index=False)
    print(f"Resultados exportados a {ruta_completa}")

def export_results_to_excel(resultados, filename="resultados.xlsx"):
    carpeta = "exportados"
    os.makedirs(carpeta, exist_ok=True)
    ruta_completa = os.path.join(carpeta, filename)
    df = pd.DataFrame(resultados)
    df.to_excel(ruta_completa, index=False)
    print(f"Resultados exportados a {ruta_completa}")

def separar_registros_coincidentes(resultados):
    coincidentes = []
    no_coincidentes = []
    for fila in resultados:
        score = fila.get('score', 0)
        if isinstance(score, str) and score.endswith('%'):
            try:
                score_val = float(score.replace('%', ''))
            except ValueError:
                score_val = 0
        else:
            score_val = float(score)
        if score_val >= 97.0:
            coincidentes.append(fila)
        else:
            no_coincidentes.append(fila)
    return coincidentes, no_coincidentes