DROP FUNCTION IF EXISTS get_next_execution;
DELIMITER $$
CREATE FUNCTION get_next_execution(cron_expr TEXT)
RETURNS DATETIME
DETERMINISTIC
BEGIN
  DECLARE cron_def JSON;
  DECLARE current DATETIME DEFAULT NOW();
  DECLARE candidate_date DATE DEFAULT CURDATE();
  DECLARE max_date DATE DEFAULT DATE_ADD(CURDATE(), INTERVAL 365 DAY);
  DECLARE ret DATETIME DEFAULT NULL;
  DECLARE found BOOLEAN DEFAULT FALSE;

  /* Startwerte */
  DECLARE start_hour INT;
  DECLARE start_minute INT;
  DECLARE start_second INT;
  DECLARE h INT;
  DECLARE m INT;
  DECLARE s INT;
  DECLARE hour_found BOOLEAN DEFAULT FALSE;
  DECLARE minute_found BOOLEAN DEFAULT FALSE;
  DECLARE second_found BOOLEAN DEFAULT FALSE;

  /* Cron-JSON-Felder */
  DECLARE cron_month JSON;
  DECLARE cron_dom JSON;
  DECLARE cron_dow JSON;
  DECLARE cron_hour JSON;
  DECLARE cron_min JSON;
  DECLARE cron_sec JSON;

  /* Vorgeparste Variablen für jedes Feld */
  DECLARE month_op VARCHAR(10);
  DECLARE month_eq INT DEFAULT 0;
  DECLARE month_in TEXT DEFAULT NULL;
  
  DECLARE dom_op VARCHAR(10);
  DECLARE dom_eq INT DEFAULT 0;
  DECLARE dom_between_start INT DEFAULT 0;
  DECLARE dom_between_end INT DEFAULT 0;
  DECLARE dom_in TEXT DEFAULT NULL;
  
  DECLARE dow_op VARCHAR(10);
  DECLARE dow_eq INT DEFAULT 0;
  DECLARE dow_in TEXT DEFAULT NULL;
  
  DECLARE hour_op VARCHAR(10);
  DECLARE hour_eq INT DEFAULT 0;
  DECLARE hour_between_start INT DEFAULT 0;
  DECLARE hour_between_end INT DEFAULT 0;
  DECLARE hour_in TEXT DEFAULT NULL;
  
  DECLARE min_op VARCHAR(10);
  DECLARE min_eq INT DEFAULT 0;
  DECLARE min_between_start INT DEFAULT 0;
  DECLARE min_between_end INT DEFAULT 0;
  DECLARE min_in TEXT DEFAULT NULL;
  
  DECLARE sec_op VARCHAR(10);
  DECLARE sec_eq INT DEFAULT 0;
  DECLARE sec_between_start INT DEFAULT 0;
  DECLARE sec_between_end INT DEFAULT 0;
  DECLARE sec_in TEXT DEFAULT NULL;

  /* Cron-Ausdruck in JSON umwandeln und Felder extrahieren */
  SET cron_def = cron_to_json(cron_expr);
  SET cron_month = JSON_EXTRACT(cron_def, '$.month');
  SET cron_dom   = JSON_EXTRACT(cron_def, '$.day_of_month');
  SET cron_dow   = JSON_EXTRACT(cron_def, '$.day_of_week');
  SET cron_hour  = JSON_EXTRACT(cron_def, '$.hour');
  SET cron_min   = JSON_EXTRACT(cron_def, '$.minute');
  SET cron_sec   = JSON_EXTRACT(cron_def, '$.second');

  /* Monat */
  SET month_op = JSON_UNQUOTE(JSON_EXTRACT(cron_month, '$.op'));
  IF month_op = '=' THEN
    SET month_eq = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_month, '$.val')) AS UNSIGNED);
  ELSEIF month_op = 'IN' THEN
    SET month_in = JSON_UNQUOTE(JSON_EXTRACT(cron_month, '$.val'));
  END IF;

  /* Tag des Monats */
  SET dom_op = JSON_UNQUOTE(JSON_EXTRACT(cron_dom, '$.op'));
  IF dom_op = '=' THEN
    SET dom_eq = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_dom, '$.val')) AS UNSIGNED);
  ELSEIF dom_op = 'BETWEEN' THEN
    SET dom_between_start = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_dom, '$.val[0]')) AS UNSIGNED);
    SET dom_between_end   = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_dom, '$.val[1]')) AS UNSIGNED);
  ELSEIF dom_op = 'IN' THEN
    SET dom_in = JSON_UNQUOTE(JSON_EXTRACT(cron_dom, '$.val'));
  END IF;

  /* Wochentag */
  SET dow_op = JSON_UNQUOTE(JSON_EXTRACT(cron_dow, '$.op'));
  IF dow_op = '=' THEN
    SET dow_eq = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_dow, '$.val')) AS UNSIGNED);
  ELSEIF dow_op = 'IN' THEN
    SET dow_in = JSON_UNQUOTE(JSON_EXTRACT(cron_dow, '$.val'));
  END IF;

  /* Stunde */
  SET hour_op = JSON_UNQUOTE(JSON_EXTRACT(cron_hour, '$.op'));
  IF hour_op = '=' THEN
    SET hour_eq = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_hour, '$.val')) AS UNSIGNED);
  ELSEIF hour_op = 'BETWEEN' THEN
    SET hour_between_start = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_hour, '$.val[0]')) AS UNSIGNED);
    SET hour_between_end   = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_hour, '$.val[1]')) AS UNSIGNED);
  ELSEIF hour_op = 'IN' THEN
    SET hour_in = JSON_UNQUOTE(JSON_EXTRACT(cron_hour, '$.val'));
  END IF;

  /* Minute */
  SET min_op = JSON_UNQUOTE(JSON_EXTRACT(cron_min, '$.op'));
  IF min_op = '=' THEN
    SET min_eq = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_min, '$.val')) AS UNSIGNED);
  ELSEIF min_op = 'BETWEEN' THEN
    SET min_between_start = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_min, '$.val[0]')) AS UNSIGNED);
    SET min_between_end   = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_min, '$.val[1]')) AS UNSIGNED);
  ELSEIF min_op = 'IN' THEN
    SET min_in = JSON_UNQUOTE(JSON_EXTRACT(cron_min, '$.val'));
  END IF;

  /* Sekunde */
  SET sec_op = JSON_UNQUOTE(JSON_EXTRACT(cron_sec, '$.op'));
  IF sec_op = '=' THEN
    SET sec_eq = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_sec, '$.val')) AS UNSIGNED);
  ELSEIF sec_op = 'BETWEEN' THEN
    SET sec_between_start = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_sec, '$.val[0]')) AS UNSIGNED);
    SET sec_between_end   = CAST(JSON_UNQUOTE(JSON_EXTRACT(cron_sec, '$.val[1]')) AS UNSIGNED);
  ELSEIF sec_op = 'IN' THEN
    SET sec_in = JSON_UNQUOTE(JSON_EXTRACT(cron_sec, '$.val'));
  END IF;

  outer_loop: WHILE candidate_date < max_date AND found = FALSE DO
    /* Monat prüfen */
    IF NOT match_value(MONTH(candidate_date), month_op, month_eq, 0, 0, month_in) THEN
      SET candidate_date = DATE_ADD(candidate_date, INTERVAL 1 DAY);
      ITERATE outer_loop;
    END IF;
    
    /* Tag und Wochentag prüfen */
    IF NOT (
         (dom_op = 'ALL' AND dow_op = 'ALL')
         OR (dom_op <> 'ALL' AND dow_op <> 'ALL' AND (
              match_value(DAYOFMONTH(candidate_date), dom_op, dom_eq, dom_between_start, dom_between_end, dom_in)
              OR match_value(WEEKDAY(candidate_date), dow_op, dow_eq, 0, 0, dow_in)
         ))
         OR (dom_op <> 'ALL' AND match_value(DAYOFMONTH(candidate_date), dom_op, dom_eq, dom_between_start, dom_between_end, dom_in))
         OR (dow_op <> 'ALL' AND match_value(WEEKDAY(candidate_date), dow_op, dow_eq, 0, 0, dow_in))
    ) THEN
      SET candidate_date = DATE_ADD(candidate_date, INTERVAL 1 DAY);
      ITERATE outer_loop;
    END IF;
    
    /* Startwerte für Stunde/Minute/Sekunde */
    IF candidate_date = CURDATE() THEN
      SET start_hour   = HOUR(current);
      SET start_minute = MINUTE(current);
      SET start_second = SECOND(current);
    ELSE
      SET start_hour   = 0;
      SET start_minute = 0;
      SET start_second = 0;
    END IF;
    
    /* Stunde ermitteln */
    SET h = start_hour;
    SET hour_found = FALSE;
    WHILE h < 24 AND hour_found = FALSE DO
      IF match_value(h, hour_op, hour_eq, hour_between_start, hour_between_end, hour_in) THEN
        SET hour_found = TRUE;
      ELSE
        SET h = h + 1;
      END IF;
    END WHILE;
    IF NOT hour_found THEN
      SET candidate_date = DATE_ADD(candidate_date, INTERVAL 1 DAY);
      ITERATE outer_loop;
    END IF;
    
    /* Minute ermitteln */
    SET m = IF(h = start_hour, start_minute, 0);
    SET minute_found = FALSE;
    WHILE m < 60 AND minute_found = FALSE DO
      IF match_value(m, min_op, min_eq, min_between_start, min_between_end, min_in) THEN
        SET minute_found = TRUE;
      ELSE
        SET m = m + 1;
      END IF;
    END WHILE;
    IF NOT minute_found THEN
      IF candidate_date = CURDATE() THEN
        SET current = DATE_ADD(DATE_SUB(DATE_SUB(current, INTERVAL MINUTE(current) MINUTE), INTERVAL SECOND(current) SECOND), INTERVAL 1 HOUR);
        SET candidate_date = DATE(current);
      ELSE
        SET candidate_date = DATE_ADD(candidate_date, INTERVAL 1 DAY);
      END IF;
      ITERATE outer_loop;
    END IF;
    
    /* Sekunde ermitteln */
    SET s = IF(h = start_hour AND m = start_minute, start_second, 0);
    SET second_found = FALSE;
    WHILE s < 60 AND second_found = FALSE DO
      IF match_value(s, sec_op, sec_eq, sec_between_start, sec_between_end, sec_in) THEN
        SET second_found = TRUE;
      ELSE
        SET s = s + 1;
      END IF;
    END WHILE;
    IF NOT second_found THEN
      IF candidate_date = CURDATE() THEN
        SET current = DATE_ADD(DATE_SUB(current, INTERVAL SECOND(current) SECOND), INTERVAL 1 MINUTE);
        SET candidate_date = DATE(current);
      ELSE
        SET candidate_date = DATE_ADD(candidate_date, INTERVAL 1 DAY);
      END IF;
      ITERATE outer_loop;
    END IF;
    
    SET ret = STR_TO_DATE(CONCAT(candidate_date, ' ', LPAD(h,2,'0'), ':', LPAD(m,2,'0'), ':', LPAD(s,2,'0')), '%Y-%m-%d %H:%i:%s');
    SET found = TRUE;
  END WHILE outer_loop;
  
  RETURN ret;
END$$
DELIMITER ;
