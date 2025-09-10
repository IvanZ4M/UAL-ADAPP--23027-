DELIMITER //
CREATE PROCEDURE sp_InsertCliente10(
    IN p_nombre VARCHAR(100),
    IN p_apellido VARCHAR(100),
    IN p_email VARCHAR(150)
)
BEGIN
    INSERT INTO clientes10 (nombre, apellido, email)
    VALUES (p_nombre, p_apellido, p_email);
END //
DELIMITER ;
