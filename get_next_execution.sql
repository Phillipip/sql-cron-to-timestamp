DROP FUNCTION IF EXISTS get_next_execution;
DELIMITER $$
CREATE FUNCTION get_next_execution(cron_expr TEXT)
RETURNS TEXT
DETERMINISTIC
BEGIN
  DECLARE ret TEXT DEFAULT NULL;
  DECLARE dt_now DATETIME;
  DECLARE candidate_dt DATETIME;
  DECLARE candidate_date DATE;
  DECLARE day_offset INT DEFAULT 0;
  
  DECLARE dom_all INT DEFAULT 0;
  DECLARE dow_all INT DEFAULT 0;
  
  DECLARE allowed_sec TEXT;
  DECLARE allowed_min TEXT;
  DECLARE allowed_hour TEXT;
  DECLARE allowed_dom TEXT;
  DECLARE allowed_month TEXT;
  DECLARE allowed_dow TEXT;
  
  DECLARE sec_field TEXT;
  DECLARE min_field TEXT;
  DECLARE hour_field TEXT;
  DECLARE dom_field TEXT;
  DECLARE month_field TEXT;
  DECLARE dow_field TEXT;
  
  DECLARE start_hour INT DEFAULT 0;
  DECLARE start_min INT DEFAULT 0;
  DECLARE start_sec INT DEFAULT 0;
  DECLARE h INT DEFAULT 0;
  DECLARE m INT DEFAULT 0;
  DECLARE s INT DEFAULT 0;
  
  -- Exakt 6 Felder voraussetzen
  IF (LENGTH(TRIM(cron_expr)) - LENGTH(REPLACE(cron_expr, ' ', '')) + 1) <> 6 THEN
         RETURN NULL;
  END IF;
  
  SET dt_now = NOW();
  
  SET sec_field = SUBSTRING_INDEX(cron_expr, ' ', 1);
  SET min_field = SUBSTRING_INDEX(SUBSTRING_INDEX(cron_expr, ' ', 2), ' ', -1);
  SET hour_field = SUBSTRING_INDEX(SUBSTRING_INDEX(cron_expr, ' ', 3), ' ', -1);
  SET dom_field = SUBSTRING_INDEX(SUBSTRING_INDEX(cron_expr, ' ', 4), ' ', -1);
  SET month_field = SUBSTRING_INDEX(SUBSTRING_INDEX(cron_expr, ' ', 5), ' ', -1);
  SET dow_field = SUBSTRING_INDEX(cron_expr, ' ', -1);
  
  -- Felder parsen
  SET allowed_sec = parse_cron_field(sec_field, 0, 59, IF(sec_field = '*', 1, 0));
  SET allowed_min = parse_cron_field(min_field, 0, 59, IF(min_field = '*', 1, 0));
  SET allowed_hour = parse_cron_field(hour_field, 0, 23, IF(hour_field = '*', 1, 0));
  SET allowed_month = parse_cron_field(month_field, 1, 12, IF(month_field = '*', 1, 0));
  
  IF dom_field = '*' THEN
       SET dom_all = 1;
  END IF;
  SET allowed_dom = parse_cron_field(dom_field, 1, 31, IF(dom_field = '*', 1, 0));
  
  IF dow_field = '*' THEN
       SET dow_all = 1;
  END IF;
  SET allowed_dow = parse_cron_field(dow_field, 0, 6, dow_all);
  
  day_loop: WHILE day_offset < 365 DO
         SET candidate_date = DATE_ADD(DATE(dt_now), INTERVAL day_offset DAY);
         -- Monat prüfen
         IF FIND_IN_SET(MONTH(candidate_date), allowed_month) = 0 THEN
             SET day_offset = day_offset + 1;
             ITERATE day_loop;
         END IF;
         -- Prüfe Tag des Monats und Wochentag gemäß Cron-Logik:
         IF (dom_all = 0 AND dow_all = 0) THEN
             IF (FIND_IN_SET(DAY(candidate_date), allowed_dom) = 0 
                 AND FIND_IN_SET(DAYOFWEEK(candidate_date)-1, allowed_dow) = 0) THEN
                 SET day_offset = day_offset + 1;
                 ITERATE day_loop;
             END IF;
         ELSE
             IF (dom_all = 0 AND FIND_IN_SET(DAY(candidate_date), allowed_dom) = 0) THEN
                 SET day_offset = day_offset + 1;
                 ITERATE day_loop;
             END IF;
             IF (dow_all = 0 AND FIND_IN_SET(DAYOFWEEK(candidate_date)-1, allowed_dow) = 0) THEN
                 SET day_offset = day_offset + 1;
                 ITERATE day_loop;
             END IF;
         END IF;
  
         IF day_offset = 0 THEN
             SET start_hour = HOUR(dt_now);
             SET start_min  = MINUTE(dt_now);
             SET start_sec  = SECOND(dt_now);
         ELSE
             SET start_hour = 0;
             SET start_min = 0;
             SET start_sec = 0;
         END IF;
  
         SET h = start_hour;
         hour_loop: WHILE h < 24 DO
             IF FIND_IN_SET(h, allowed_hour) = 0 THEN
                 SET h = h + 1;
                 ITERATE hour_loop;
             END IF;
             SET m = IF(h = start_hour, start_min, 0);
             minute_loop: WHILE m < 60 DO
                 IF FIND_IN_SET(m, allowed_min) = 0 THEN
                     SET m = m + 1;
                     ITERATE minute_loop;
                 END IF;
                 SET s = IF(h = start_hour AND m = start_min, start_sec, 0);
                 second_loop: WHILE s < 60 DO
                     IF FIND_IN_SET(s, allowed_sec) > 0 THEN
                         SET candidate_dt = DATE_ADD(
                                DATE_ADD(
                                  DATE_ADD(candidate_date, INTERVAL h HOUR),
                                  INTERVAL m MINUTE),
                                INTERVAL s SECOND);
                         IF candidate_dt >= dt_now THEN
                             SET ret = DATE_FORMAT(candidate_dt, '%Y-%m-%d %H:%i:%s');
                             LEAVE day_loop;
                         END IF;
                     END IF;
                     SET s = s + 1;
                 END WHILE second_loop;
                 SET m = m + 1;
             END WHILE minute_loop;
             SET h = h + 1;
         END WHILE hour_loop;
         SET day_offset = day_offset + 1;
  END WHILE day_loop;
  
  RETURN ret;
END$$
DELIMITER ;
