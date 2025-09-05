import mysql.connector

def execute_sql_file(filename, connection_params):
    # Ejecuta un archivo SQL
    try:
        conn = mysql.connector.connect(**connection_params)
        cursor = conn.cursor()
        
        with open(filename, 'r', encoding='utf-8') as file:
            sql_script = file.read()
        
        # Ejecutar cada statement separado por ;
        for statement in sql_script.split(';'):
            if statement.strip():
                cursor.execute(statement)
        
        conn.commit()
        print(f"Script {filename} ejecutado correctamente")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error ejecutando {filename}: {e}")

# Configuración de conexión
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "port": 3306
}

# Ejecutar scripts sp
execute_sql_file("sp_InsertClientesCSV_crm_27551.sql", {**db_config, "database": "crm"})
execute_sql_file("sp_InsertUsuariosCSV_dbo_27551.sql", {**db_config, "database": "dbo"})