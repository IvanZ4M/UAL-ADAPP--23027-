DELIMITER //
CREATE PROCEDURE sp_InsertCoincidente(
    IN p_nombre VARCHAR(100),
    IN p_apellido VARCHAR(100),
    IN p_match_query TEXT,
    IN p_match_result TEXT,
    IN p_score FLOAT,
    IN p_match_result_values TEXT,
    IN p_destTable VARCHAR(100),
    IN p_sourceTable VARCHAR(100)
)
BEGIN
    DECLARE next_num INT;
    DECLARE new_numero_control VARCHAR(6);
    SELECT IFNULL(MAX(CAST(SUBSTRING(numero_control, 3) AS UNSIGNED)), 0) + 1 INTO next_num FROM Coincidentes;
    SET new_numero_control = CONCAT('DR', LPAD(next_num, 4, '0'));
    INSERT INTO Coincidentes (
        numero_control, nombre, apellido, match_query, match_result, score, match_result_values, destTable, sourceTable
    ) VALUES (
        new_numero_control, p_nombre, p_apellido, p_match_query, p_match_result, p_score, p_match_result_values, p_destTable, p_sourceTable
    );
END //
DELIMITER ;
