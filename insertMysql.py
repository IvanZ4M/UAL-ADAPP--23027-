import csv
import mysql.connector
from datetime import datetime

# Configuración para la base de datos CRM
try:
    conn_crm = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",  # XAMPP por defecto tiene password vacío
        database="crm",
        port=3306  # Puerto por defecto de MariaDB en XAMPP
    )
    cursor_crm = conn_crm.cursor()
    
    with open("clientes.csv", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            fecha = datetime.strptime(row["fecha_registro"], "%d/%m/%Y %H:%M")
            
            # Llamar al procedimiento almacenado
            cursor_crm.callproc('sp_InsertClientesCSV_crm_001', [
                int(row["cliente_id"]),
                row["nombre"],
                row["apellido"],
                row["email"],
                fecha
            ])
    
    conn_crm.commit()
    print("Clientes insertados en DB crm mediante procedimiento almacenado.")
    cursor_crm.close()
    conn_crm.close()
    
except Exception as e:
    print(f"Error procesando clientes: {e}")

# Configuración para la base de datos dbo
try:
    conn_dbo = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",  # XAMPP por defecto tiene password vacío
        database="dbo",
        port=3306  # Puerto por defecto de MariaDB en XAMPP
    )
    cursor_dbo = conn_dbo.cursor()
    
    with open("usuarios.csv", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            fecha = datetime.strptime(row["fecha_creacion"], "%d/%m/%Y %H:%M")
            
            # Llamar al procedimiento almacenado
            cursor_dbo.callproc('sp_InsertUsuariosCSV_dbo_001', [
                int(row["userId"]),
                row["username"],
                row["first_name"],
                row["last_name"],
                row["email"],
                row["password_hash"],
                row["rol"],
                fecha
            ])
    
    conn_dbo.commit()
    print("Usuarios insertados en DB dbo mediante procedimiento almacenado.")
    cursor_dbo.close()
    conn_dbo.close()
    
except Exception as e:
    print(f"Error procesando usuarios: {e}")