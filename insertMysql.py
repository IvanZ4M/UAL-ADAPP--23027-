import csv
import mysql.connector
from datetime import datetime

# Insertar en tabla Clientes 
conn_crm = mysql.connector.connect(
    host="localhost",
    user="root",
    database="crm"
)
cursor_crm = conn_crm.cursor()

with open("clientes.csv", newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        try:
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
        except Exception as e:
            print(f"Error insertando cliente {row['cliente_id']}: {e}")

conn_crm.commit()
print("Clientes insertados en DB crm.")
cursor_crm.close()
conn_crm.close()


# Insertar en tabla Usuarios 
conn_dbo = mysql.connector.connect(
    host="localhost",
    user="root",
    database="dbo"
)
cursor_dbo = conn_dbo.cursor()

with open("usuarios.csv", newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        try:
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
        except Exception as e:
            print(f"Error insertando usuario {row['username']}: {e}")

conn_dbo.commit()
print("Usuarios insertados en DB dbo.")
cursor_dbo.close()
conn_dbo.close()
