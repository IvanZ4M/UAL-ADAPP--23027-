DELIMITER //

CREATE PROCEDURE sp_InsertClientesCSV_crm_001(
    IN p_cliente_id INT,
    IN p_nombre VARCHAR(100),
    IN p_apellido VARCHAR(100),
    IN p_email VARCHAR(150),
    IN p_fecha_registro DATETIME
)
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Error al insertar/actualizar cliente';
    END;
    
    START TRANSACTION;
    
    -- Verificar si el cliente_id ya existe
    IF EXISTS (SELECT 1 FROM Clientes WHERE cliente_id = p_cliente_id) THEN
        -- Actualizar cliente existente
        UPDATE Clientes 
        SET nombre = p_nombre, 
            apellido = p_apellido, 
            email = p_email, 
            FechaRegistro = p_fecha_registro
        WHERE cliente_id = p_cliente_id;
    ELSE
        -- Insertar nuevo cliente
        INSERT INTO Clientes (cliente_id, nombre, apellido, email, FechaRegistro)
        VALUES (p_cliente_id, p_nombre, p_apellido, p_email, p_fecha_registro);
    END IF;
    
    COMMIT;
END //

DELIMITER ;