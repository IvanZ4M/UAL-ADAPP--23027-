DELIMITER //
CREATE PROCEDURE sp_InsertCliente10(
    IN p_nombre VARCHAR(100),
    IN p_apellido VARCHAR(100),
    IN p_email VARCHAR(150),
    IN p_match_query VARCHAR(255),
    IN p_match_result VARCHAR(255),
    IN p_score FLOAT,
    IN p_match_result_values TEXT,
    IN p_destTable VARCHAR(100),
    IN p_sourceTable VARCHAR(100),
    IN p_fecha DATETIME
)
BEGIN
    INSERT INTO clientes10 (nombre, apellido, email, match_query, match_result, score, match_result_values, destTable, sourceTable, fecha)
    VALUES (p_nombre, p_apellido, p_email, p_match_query, p_match_result, p_score, p_match_result_values, p_destTable, p_sourceTable, p_fecha);
END //
DELIMITER ;
