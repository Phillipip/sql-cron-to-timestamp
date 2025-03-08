DROP FUNCTION IF EXISTS parse_cron_field;
DELIMITER $$
CREATE FUNCTION parse_cron_field(field TEXT, min_val INT, max_val INT, out_is_all INT)
RETURNS TEXT
DETERMINISTIC
BEGIN
    DECLARE result TEXT DEFAULT '';
    DECLARE token TEXT;
    DECLARE comma_pos INT;
    DECLARE step INT DEFAULT 1;
    DECLARE start_val INT;
    DECLARE end_val INT;
    DECLARE dash_pos INT;
    DECLARE slash_pos INT;
    
    -- Falls das gesamte Feld "*" ist, alle Werte generieren
    IF field = '*' THEN
        SET out_is_all = 1;
        WHILE min_val <= max_val DO
            SET result = IF(result = '', CAST(min_val AS CHAR), CONCAT(result, ',', CAST(min_val AS CHAR)));
            SET min_val = min_val + 1;
        END WHILE;
        RETURN result;
    END IF;
    
    SET out_is_all = 0;
    WHILE field <> '' DO
        SET comma_pos = LOCATE(',', field);
        IF comma_pos > 0 THEN
            SET token = TRIM(SUBSTRING(field, 1, comma_pos - 1));
            SET field = SUBSTRING(field, comma_pos + 1);
        ELSE
            SET token = TRIM(field);
            SET field = '';
        END IF;
        
        -- Standardwerte
        SET step = 1;
        -- Prüfe, ob ein Schritt angegeben wurde
        SET slash_pos = LOCATE('/', token);
        IF slash_pos > 0 THEN
            SET step = CAST(SUBSTRING(token, slash_pos+1) AS UNSIGNED);
            IF step <= 0 THEN 
                SET step = 1; 
            END IF;
            SET token = TRIM(SUBSTRING(token, 1, slash_pos - 1));
        END IF;
        
        -- Bestimme Bereich
        SET dash_pos = LOCATE('-', token);
        IF dash_pos > 0 THEN
            SET start_val = CAST(SUBSTRING(token, 1, dash_pos - 1) AS UNSIGNED);
            SET end_val = CAST(SUBSTRING(token, dash_pos+1) AS UNSIGNED);
        ELSE
            IF token = '' OR token = '*' THEN
                -- Bei einem leeren Token oder "*" vor dem Schrägstrich: Range von min_val bis max_val
                SET start_val = min_val;
                SET end_val = max_val;
            ELSE
                -- Andernfalls: Wenn nur ein einzelner Wert angegeben wurde, gilt dieser als Start
                SET start_val = CAST(token AS UNSIGNED);
                -- Falls ein Schritt angegeben war, soll als Endwert max_val genutzt werden
                IF step > 1 THEN
                    SET end_val = max_val;
                ELSE
                    SET end_val = start_val;
                END IF;
            END IF;
        END IF;
        
        IF start_val < min_val THEN SET start_val = min_val; END IF;
        IF end_val > max_val THEN SET end_val = max_val; END IF;
        
        WHILE start_val <= end_val DO
            IF FIND_IN_SET(start_val, result) = 0 THEN
                SET result = IF(result = '', CAST(start_val AS CHAR), CONCAT(result, ',', CAST(start_val AS CHAR)));
            END IF;
            SET start_val = start_val + step;
        END WHILE;
    END WHILE;
    RETURN result;
END$$
DELIMITER ;
