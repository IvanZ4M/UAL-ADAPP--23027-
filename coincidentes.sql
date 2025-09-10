CREATE TABLE IF NOT EXISTS Coincidentes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    numero_control VARCHAR(6) UNIQUE,
    nombre VARCHAR(100) NULL,
    apellido VARCHAR(100) NULL,
    match_query TEXT NULL,
    match_result TEXT NULL,
    score FLOAT NULL,
    match_result_values TEXT NULL,
    destTable VARCHAR(100) NULL,
    sourceTable VARCHAR(100) NULL,
    fecha_insercion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);