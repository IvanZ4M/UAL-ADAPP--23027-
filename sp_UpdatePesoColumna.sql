CREATE TABLE IF NOT EXISTS ConfigPesos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    columna VARCHAR(100) UNIQUE NOT NULL,
    peso INT NOT NULL,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DELIMITER //
CREATE PROCEDURE sp_UpdatePesoColumna(
    IN p_columna VARCHAR(100),
    IN p_peso INT
)
BEGIN
    UPDATE ConfigPesos SET peso = p_peso, fecha_actualizacion = CURRENT_TIMESTAMP WHERE columna = p_columna;
    IF ROW_COUNT() = 0 THEN
        INSERT INTO ConfigPesos (columna, peso) VALUES (p_columna, p_peso);
    END IF;
END //
DELIMITER ;