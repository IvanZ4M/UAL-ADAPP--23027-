CREATE DATABASE IF NOT EXISTS crm;
USE crm;

CREATE TABLE Clientes (
    cliente_id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    apellido VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    match_query VARCHAR(255) NULL,
    match_result VARCHAR(255) NULL,
    score FLOAT NULL,
    match_result_values TEXT NULL,
    destTable VARCHAR(100) NULL,
    sourceTable VARCHAR(100) NULL,
    fecha DATETIME NULL,
    FechaRegistro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
