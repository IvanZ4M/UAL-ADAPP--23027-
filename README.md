##  Documentación de Funciones

---

### **1. `connect_to_azure_sql(server, database, username, password)`**
Conecta a la base de datos **Azure SQL** usando ODBC.

**Parámetros:**
- `server` *(str)* → Dirección del servidor SQL.
- `database` *(str)* → Nombre de la base de datos.
- `username` *(str)* → Usuario para la conexión.
- `password` *(str)* → Contraseña del usuario.

**Retorno:**
- Objeto **`pyodbc.Connection`** que permite ejecutar consultas SQL.

---

### **2. `fuzzy_match(queryRecord, choices, score_cutoff=0)`**
Encuentra la mejor coincidencia entre un texto (**registro origen**) y una lista de opciones (**registros destino**) usando algoritmos de similitud.

**Parámetros:**
- `queryRecord` *(str)* → Cadena base a comparar.
- `choices` *(list[dict])* → Lista de diccionarios que representan los registros destino.
- `score_cutoff` *(int, opcional)* → Puntaje mínimo para aceptar la coincidencia (**0-100**).

**Qué hace:**
- Convierte el registro origen en texto.
- Compara con cada registro destino concatenado.
- Usa varios algoritmos: **WRatio**, **QRatio**, **token_set_ratio**, **ratio**.
- Devuelve la mejor coincidencia encontrada.

**Retorno:**
- `dict` con:
  - `match_query` → Texto del registro origen.
  - `match_result` → Texto del registro destino coincidente.
  - `score` → Puntaje de similitud (**0-100**).
  - `match_result_values` → Valores del registro destino.

---

### **3. `execute_dynamic_matching(params_dict, score_cutoff=0)`**
Compara dinámicamente dos tablas en **Azure SQL** y devuelve las mejores coincidencias para cada registro.

**Parámetros:**
- `params_dict` *(dict)* con:
  - `server` → Servidor SQL.
  - `database` → Nombre de la base de datos.
  - `username` → Usuario de conexión.
  - `password` → Contraseña.
  - `sourceSchema` → Esquema de la tabla origen.
  - `sourceTable` → Nombre de la tabla origen.
  - `destSchema` → Esquema de la tabla destino.
  - `destTable` → Nombre de la tabla destino.
  - `src_dest_mappings` → Diccionario con columnas **origen → destino**.
- `score_cutoff` *(int, opcional)* → Puntaje mínimo para aceptar coincidencias.

**Qué hace:**
1. Se conecta a la base de datos Azure SQL.
2. Obtiene datos de la tabla origen y destino.
3. Construye cadenas concatenadas para comparar.
4. Usa **`fuzzy_match()`** para encontrar la mejor coincidencia para cada registro.
5. Devuelve los resultados con detalle de origen, destino y score.

**Retorno:**
- `list[dict]` con:
  - Valores del registro origen.
  - Mejor coincidencia encontrada.
  - Puntaje de similitud.
  - Tablas origen y destino.

---
