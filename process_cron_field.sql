DROP FUNCTION IF EXISTS process_cron_field;
DELIMITER $$
CREATE FUNCTION process_cron_field(field TEXT, min_val INT, max_val INT)
RETURNS JSON
DETERMINISTIC
BEGIN
  DECLARE base_val INT;
  DECLARE step INT;
  DECLARE lst TEXT DEFAULT '';
  DECLARE i INT;
  DECLARE comma_array JSON;
  DECLARE arr_length INT;
  
  -- Variablen für den Komma-Zweig
  DECLARE splitted TEXT DEFAULT '';
  DECLARE part TEXT DEFAULT '';
  DECLARE result TEXT DEFAULT '';
  DECLARE tmp TEXT DEFAULT '';
  DECLARE start_val INT;
  DECLARE end_val INT;
  
  IF field = '*' THEN
    RETURN JSON_OBJECT('op', 'ALL');
    
  ELSEIF field LIKE '%,%' THEN
    SET splitted = field;
    WHILE splitted <> '' DO
      IF LOCATE(',', splitted) > 0 THEN
        SET part = SUBSTRING_INDEX(splitted, ',', 1);
        SET splitted = SUBSTRING(splitted, LOCATE(',', splitted) + 1);
      ELSE
        SET part = splitted;
        SET splitted = '';
      END IF;
      
      IF part LIKE '%/%' THEN
        IF part LIKE '%-%' THEN
          -- Kombinierter Fall: Bereich mit Schritt, z.B. 5-20/5
          SET start_val = CAST(SUBSTRING_INDEX(part, '-', 1) AS UNSIGNED);
          SET end_val = CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(part, '/', 1), '-', -1) AS UNSIGNED);
          SET step = CAST(SUBSTRING_INDEX(part, '/', -1) AS UNSIGNED);
          SET i = start_val;
          SET tmp = '';
          WHILE i <= end_val DO
            IF tmp = '' THEN
              SET tmp = CAST(i AS CHAR);
            ELSE
              SET tmp = CONCAT(tmp, ',', CAST(i AS CHAR));
            END IF;
            SET i = i + step;
          END WHILE;
          SET part = tmp;
        ELSE
          -- Nur Schrittangabe, z.B. */5 oder 5/5
          IF LEFT(part, 1) = '*' THEN 
            SET base_val = min_val;
          ELSE 
            SET base_val = CAST(SUBSTRING_INDEX(part, '/', 1) AS UNSIGNED);
          END IF;
          SET step = CAST(SUBSTRING_INDEX(part, '/', -1) AS UNSIGNED);
          SET i = base_val;
          SET tmp = '';
          WHILE i <= max_val DO
            IF tmp = '' THEN
              SET tmp = CAST(i AS CHAR);
            ELSE
              SET tmp = CONCAT(tmp, ',', CAST(i AS CHAR));
            END IF;
            SET i = i + step;
          END WHILE;
          SET part = tmp;
        END IF;
      ELSEIF part LIKE '%-%' THEN
        SET start_val = CAST(SUBSTRING_INDEX(part, '-', 1) AS UNSIGNED);
        SET end_val = CAST(SUBSTRING_INDEX(part, '-', -1) AS UNSIGNED);
        SET tmp = '';
        WHILE start_val <= end_val DO
          IF tmp = '' THEN
            SET tmp = CAST(start_val AS CHAR);
          ELSE
            SET tmp = CONCAT(tmp, ',', CAST(start_val AS CHAR));
          END IF;
          SET start_val = start_val + 1;
        END WHILE;
        SET part = tmp;
      END IF;
      
      IF result = '' THEN
        SET result = part;
      ELSE
        SET result = CONCAT(result, ',', part);
      END IF;
    END WHILE;
    RETURN JSON_OBJECT('op', 'IN', 'val', result);
    
  ELSEIF field LIKE '%/%' THEN
    IF field LIKE '%-%' THEN
      -- Kombinierter Fall: Bereich mit Schritt, z.B. 5-20/5
      SET start_val = CAST(SUBSTRING_INDEX(field, '-', 1) AS UNSIGNED);
      SET end_val = CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(field, '/', 1), '-', -1) AS UNSIGNED);
      SET step = CAST(SUBSTRING_INDEX(field, '/', -1) AS UNSIGNED);
      SET i = start_val;
      SET lst = '';
      WHILE i <= end_val DO
        IF lst = '' THEN
          SET lst = CAST(i AS CHAR);
        ELSE
          SET lst = CONCAT(lst, ',', CAST(i AS CHAR));
        END IF;
        SET i = i + step;
      END WHILE;
      RETURN JSON_OBJECT('op', 'IN', 'val', lst);
    ELSE
      IF LEFT(field, 1) = '*' THEN 
        SET base_val = min_val;
      ELSE 
        SET base_val = CAST(SUBSTRING_INDEX(field, '/', 1) AS UNSIGNED);
      END IF;
      SET step = CAST(SUBSTRING_INDEX(field, '/', -1) AS UNSIGNED);
      SET i = base_val;
      SET lst = '';
      WHILE i <= max_val DO
        IF lst = '' THEN
          SET lst = CAST(i AS CHAR);
        ELSE
          SET lst = CONCAT(lst, ',', CAST(i AS CHAR));
        END IF;
        SET i = i + step;
      END WHILE;
      RETURN JSON_OBJECT('op', 'IN', 'val', lst);
    END IF;
    
  ELSEIF field LIKE '%-%' THEN
    RETURN JSON_OBJECT('op', 'BETWEEN', 'val', JSON_EXTRACT(JSON_ARRAY(
      CAST(SUBSTRING_INDEX(field, '-', 1) AS UNSIGNED),
      CAST(SUBSTRING_INDEX(field, '-', -1) AS UNSIGNED)
    ), '$'));
    
  ELSE
    RETURN JSON_OBJECT('op', '=', 'val', field);
  END IF;
END$$
DELIMITER ;
