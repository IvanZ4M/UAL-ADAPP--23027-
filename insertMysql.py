import csv
import mysql.connector
from datetime import datetime

with mysql.connector.connect(
    host="localhost",
    user="root",
    database="crm"
) as conn_crm:
    cursor_crm = conn_crm.cursor()
    try:
        with open("clientes.csv", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                fecha = datetime.strptime(row["fecha_registro"], "%d/%m/%Y %H:%M")
                sql = """
                    INSERT INTO Clientes (cliente_id, nombre, apellido, email, FechaRegistro)
                    VALUES (%s, %s, %s, %s, %s)
                """
                valores = (
                    int(row["cliente_id"]),
                    row["nombre"],
                    row["apellido"],
                    row["email"],
                    fecha
                )
                cursor_crm.execute(sql, valores)
        conn_crm.commit()
        print("Clientes insertados en DB crm.")
    except Exception as e:
        print(f"Error procesando clientes: {e}")


with mysql.connector.connect(
    host="localhost",
    user="root",
    database="dbo"
) as conn_dbo:
    cursor_dbo = conn_dbo.cursor()
    try:
        with open("usuarios.csv", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                fecha = datetime.strptime(row["fecha_creacion"], "%d/%m/%Y %H:%M")
                sql = """
                    INSERT INTO Usuarios 
                    (userId, username, first_name, last_name, email, password_hash, rol, fecha_creacion)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """
                valores = (
                    int(row["userId"]),
                    row["username"],
                    row["first_name"],
                    row["last_name"],
                    row["email"],
                    row["password_hash"],
                    row["rol"],
                    fecha
                )
                cursor_dbo.execute(sql, valores)
        conn_dbo.commit()
        print("Usuarios insertados en DB dbo.")
    except Exception as e:
        print(f"Error procesando usuarios: {e}")