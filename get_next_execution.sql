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

    /* Variablen für Startwerte in Stunde/Minute/Sekunde */
    DECLARE start_hour INT;
    DECLARE start_minute INT;
    DECLARE start_second INT;

    /* Temporäre Variablen für gefundene Stunde/Minute/Sekunde */
    DECLARE h INT;
    DECLARE m INT;
    DECLARE s INT;

    /* Hilfs-Variablen */
    DECLARE hour_found   BOOLEAN DEFAULT FALSE;
    DECLARE minute_found BOOLEAN DEFAULT FALSE;
    DECLARE second_found BOOLEAN DEFAULT FALSE;

    /* JSON-Felder nur einmalig auslesen */
    DECLARE cron_month JSON;
    DECLARE cron_dom   JSON;
    DECLARE cron_dow   JSON;
    DECLARE cron_hour  JSON;
    DECLARE cron_min   JSON;
    DECLARE cron_sec   JSON;

    /* Operatoren für day_of_month / day_of_week */
    DECLARE dom_op VARCHAR(10);
    DECLARE dow_op VARCHAR(10);

    /* Cron-Ausdruck nach Doppelpunkt extrahieren + JSON */
    SET cron_def  = cron_to_json(cron_expr);

    /* Nur einmalig die relevanten Felder aus dem JSON holen */
    SET cron_month = JSON_EXTRACT(cron_def, '$.month');
    SET cron_dom   = JSON_EXTRACT(cron_def, '$.day_of_month');
    SET cron_dow   = JSON_EXTRACT(cron_def, '$.day_of_week');
    SET cron_hour  = JSON_EXTRACT(cron_def, '$.hour');
    SET cron_min   = JSON_EXTRACT(cron_def, '$.minute');
    SET cron_sec   = JSON_EXTRACT(cron_def, '$.second');

    /* Operatoren für Tag-des-Monats + Wochentag (z. B. 'ALL') ermitteln */
    SET dom_op = JSON_UNQUOTE(JSON_EXTRACT(cron_dom, '$.op'));
    SET dow_op = JSON_UNQUOTE(JSON_EXTRACT(cron_dow, '$.op'));

    /* outer_loop: max. 365 Tage */
    outer_loop: WHILE candidate_date < max_date AND found = FALSE DO

        /* Monat checken */
        IF NOT match_value(MONTH(candidate_date), cron_month) THEN
            SET candidate_date = DATE_ADD(candidate_date, INTERVAL 1 DAY);
            ITERATE outer_loop;
        END IF;

        /* Große OR-Bedingung für Tag_des_Monats und day_of_week */
        IF match_value(MONTH(candidate_date), cron_month) AND (
					(dom_op = 'ALL' AND dow_op = 'ALL') OR (
					dom_op <> 'ALL' AND dow_op <> 'ALL' AND (
						match_value(DAYOFMONTH(candidate_date), cron_dom)
						OR match_value(WEEKDAY(candidate_date), cron_dow)
					)) OR (
						dom_op <> 'ALL' AND match_value(DAYOFMONTH(candidate_date), cron_dom)
					) OR (
						dow_op <> 'ALL' AND match_value(WEEKDAY(candidate_date), cron_dow)
					)
				)
        THEN
            /* Heute => ab aktuellem Zeitpunkt, sonst ab 00:00:00 */
            IF candidate_date = CURDATE() THEN
                SET start_hour   = HOUR(current);
                SET start_minute = MINUTE(current);
                SET start_second = SECOND(current);
            ELSE
                SET start_hour   = 0;
                SET start_minute = 0;
                SET start_second = 0;
            END IF;

            /* Stunde finden */
            SET h = start_hour;
            SET hour_found = FALSE;
            WHILE h < 24 AND hour_found = FALSE DO
                IF match_value(h, cron_hour) THEN
                    SET hour_found = TRUE; 
                ELSE
                    SET h = h + 1;
                END IF;
            END WHILE;

            IF hour_found = FALSE THEN
                SET candidate_date = DATE_ADD(candidate_date, INTERVAL 1 DAY);
                ITERATE outer_loop;
            END IF;

            /* Minute finden */
            SET m = IF(h = start_hour, start_minute, 0);
            SET minute_found = FALSE;
            WHILE m < 60 AND minute_found = FALSE DO
                IF match_value(m, cron_min) THEN
                    SET minute_found = TRUE;
                ELSE
                    SET m = m + 1;
                END IF;
            END WHILE;

            IF minute_found = FALSE THEN
								IF candidate_date = CURDATE() THEN
									SET current = DATE_ADD(DATE_SUB(DATE_SUB(current, INTERVAL MINUTE(current) MINUTE), INTERVAL SECOND(current) SECOND), INTERVAL 1 HOUR);
									SET candidate_date = DATE(current);
								ELSE
									SET candidate_date = DATE_ADD(candidate_date, INTERVAL 1 DAY);
								END IF;
                ITERATE outer_loop;
            END IF;

            /* Sekunde finden */
            SET s = IF(h = start_hour AND m = start_minute, start_second, 0);
            SET second_found = FALSE;
            WHILE s < 60 AND second_found = FALSE DO
                IF match_value(s, cron_sec) THEN
                    SET second_found = TRUE;
                ELSE
                    SET s = s + 1;
                END IF;
            END WHILE;

            IF second_found = FALSE THEN
								IF candidate_date = CURDATE() THEN
									SET current = DATE_ADD(DATE_SUB(current, INTERVAL SECOND(current) SECOND), INTERVAL 1 MINUTE);
									SET candidate_date = DATE(current);
								ELSE
		              SET candidate_date = DATE_ADD(candidate_date, INTERVAL 1 DAY);
								END IF;
                ITERATE outer_loop;
            END IF;

            /* h, m, s -> finaler Timestamp */
            SET ret = STR_TO_DATE(CONCAT(candidate_date, ' ', LPAD(h,2,'0'), ':', LPAD(m,2,'0'), ':', LPAD(s,2,'0')), '%Y-%m-%d %H:%i:%s');
            SET found = TRUE;
        END IF;

        IF found = FALSE THEN
            SET candidate_date = DATE_ADD(candidate_date, INTERVAL 1 DAY);
        END IF;

    END WHILE outer_loop;

    RETURN ret;
END$$
DELIMITER ;
