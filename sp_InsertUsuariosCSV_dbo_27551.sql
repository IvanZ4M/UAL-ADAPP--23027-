DELIMITER //

CREATE PROCEDURE sp_InsertUsuariosCSV_dbo_001(
    IN p_userId INT,
    IN p_username VARCHAR(100),
    IN p_first_name VARCHAR(100),
    IN p_last_name VARCHAR(100),
    IN p_email VARCHAR(150),
    IN p_password_hash VARCHAR(255),
    IN p_rol VARCHAR(50),
    IN p_fecha_creacion DATETIME
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- Manejo de errores
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- Verificar si el userId ya existe
    IF NOT EXISTS (SELECT 1 FROM Usuarios WHERE userId = p_userId) THEN
        -- Insertar nuevo usuario
        INSERT INTO Usuarios (userId, username, first_name, last_name, email, password_hash, rol, fecha_creacion)
        VALUES (p_userId, p_username, p_first_name, p_last_name, p_email, p_password_hash, p_rol, p_fecha_creacion);
    ELSE
        -- Actualizar usuario existente
        UPDATE Usuarios 
        SET username = p_username,
            first_name = p_first_name,
            last_name = p_last_name,
            email = p_email,
            password_hash = p_password_hash,
            rol = p_rol,
            fecha_creacion = p_fecha_creacion
        WHERE userId = p_userId;
    END IF;
    
    COMMIT;
END //

DELIMITER ;