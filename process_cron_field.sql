DROP FUNCTION IF EXISTS process_cron_field;
DELIMITER $$
CREATE FUNCTION process_cron_field(field TEXT, min_val INT, max_val INT)
RETURNS JSON
DETERMINISTIC
BEGIN
  DECLARE result TEXT DEFAULT '';
  DECLARE part TEXT;
  DECLARE tmp TEXT;
  DECLARE i INT;
  DECLARE start_val INT;
  DECLARE end_val INT;
  DECLARE step INT;
  DECLARE splitted TEXT DEFAULT field;
  
  IF field = '*' THEN
    RETURN JSON_OBJECT('op','ALL');
  END IF;
  
  IF LOCATE(',', field) > 0 THEN
    WHILE splitted <> '' DO
      IF LOCATE(',', splitted) > 0 THEN
        SET part = SUBSTRING_INDEX(splitted, ',', 1);
        SET splitted = SUBSTRING(splitted, LOCATE(',', splitted) + 1);
      ELSE
        SET part = splitted;
        SET splitted = '';
      END IF;
      
      IF LOCATE('/', part) > 0 THEN
        IF LOCATE('-', part) > 0 THEN
          SET start_val = CAST(SUBSTRING_INDEX(part, '-', 1) AS UNSIGNED);
          SET end_val = CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(part, '/', 1), '-', -1) AS UNSIGNED);
          SET step = CAST(SUBSTRING_INDEX(part, '/', -1) AS UNSIGNED);
          SET i = start_val;
          SET tmp = '';
          WHILE i <= end_val DO
            SET tmp = IF(tmp = '', CAST(i AS CHAR), CONCAT(tmp, ',', CAST(i AS CHAR)));
            SET i = i + step;
          END WHILE;
          SET part = tmp;
        ELSE
          IF LEFT(part,1) = '*' THEN 
            SET i = min_val;
          ELSE 
            SET i = CAST(SUBSTRING_INDEX(part, '/', 1) AS UNSIGNED);
          END IF;
          SET step = CAST(SUBSTRING_INDEX(part, '/', -1) AS UNSIGNED);
          SET tmp = '';
          WHILE i <= max_val DO
            SET tmp = IF(tmp = '', CAST(i AS CHAR), CONCAT(tmp, ',', CAST(i AS CHAR)));
            SET i = i + step;
          END WHILE;
          SET part = tmp;
        END IF;
      ELSEIF LOCATE('-', part) > 0 THEN
        SET start_val = CAST(SUBSTRING_INDEX(part, '-', 1) AS UNSIGNED);
        SET end_val = CAST(SUBSTRING_INDEX(part, '-', -1) AS UNSIGNED);
        SET tmp = '';
        WHILE start_val <= end_val DO
          SET tmp = IF(tmp = '', CAST(start_val AS CHAR), CONCAT(tmp, ',', CAST(start_val AS CHAR)));
          SET start_val = start_val + 1;
        END WHILE;
        SET part = tmp;
      END IF;
      
      SET result = IF(result = '', part, CONCAT(result, ',', part));
    END WHILE;
    RETURN JSON_OBJECT('op','IN','val', result);
    
  ELSEIF LOCATE('/', field) > 0 THEN
    IF LOCATE('-', field) > 0 THEN
      SET start_val = CAST(SUBSTRING_INDEX(field, '-', 1) AS UNSIGNED);
      SET end_val = CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(field, '/', 1), '-', -1) AS UNSIGNED);
      SET step = CAST(SUBSTRING_INDEX(field, '/', -1) AS UNSIGNED);
      SET i = start_val;
      SET result = '';
      WHILE i <= end_val DO
        SET result = IF(result = '', CAST(i AS CHAR), CONCAT(result, ',', CAST(i AS CHAR)));
        SET i = i + step;
      END WHILE;
      RETURN JSON_OBJECT('op','IN','val', result);
    ELSE
      IF LEFT(field,1) = '*' THEN
        SET i = min_val;
      ELSE
        SET i = CAST(SUBSTRING_INDEX(field, '/', 1) AS UNSIGNED);
      END IF;
      SET step = CAST(SUBSTRING_INDEX(field, '/', -1) AS UNSIGNED);
      SET result = '';
      WHILE i <= max_val DO
        SET result = IF(result = '', CAST(i AS CHAR), CONCAT(result, ',', CAST(i AS CHAR)));
        SET i = i + step;
      END WHILE;
      RETURN JSON_OBJECT('op','IN','val', result);
    END IF;
    
  ELSEIF LOCATE('-', field) > 0 THEN
    RETURN JSON_OBJECT('op','BETWEEN','val', JSON_EXTRACT(JSON_ARRAY(
      CAST(SUBSTRING_INDEX(field, '-', 1) AS UNSIGNED),
      CAST(SUBSTRING_INDEX(field, '-', -1) AS UNSIGNED)
    ), '$'));
    
  ELSE
    RETURN JSON_OBJECT('op','=', 'val', field);
  END IF;
END$$
DELIMITER ;
