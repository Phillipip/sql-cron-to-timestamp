DROP FUNCTION IF EXISTS match_value;
DELIMITER $$
CREATE FUNCTION match_value(candidate INT, cond JSON) RETURNS BOOLEAN
DETERMINISTIC
BEGIN
    DECLARE op VARCHAR(20);
    DECLARE val0 INT;
    DECLARE val1 INT;
    SET op = JSON_UNQUOTE(JSON_EXTRACT(cond, '$.op'));
    
    IF op = 'ALL' THEN
        RETURN TRUE;
    ELSEIF op = '=' THEN
        SET val0 = CAST(JSON_UNQUOTE(JSON_EXTRACT(cond, '$.val')) AS UNSIGNED);
        RETURN candidate = val0;
    ELSEIF op = 'BETWEEN' THEN
        SET val0 = CAST(JSON_UNQUOTE(JSON_EXTRACT(cond, '$.val[0]')) AS UNSIGNED);
        SET val1 = CAST(JSON_UNQUOTE(JSON_EXTRACT(cond, '$.val[1]')) AS UNSIGNED);
        RETURN candidate BETWEEN val0 AND val1;
    ELSEIF op = 'IN' THEN
        RETURN FIND_IN_SET(candidate, JSON_UNQUOTE(JSON_EXTRACT(cond, '$.val'))) > 0;
    ELSE
        RETURN FALSE;
    END IF;
END$$
DELIMITER ;

DROP FUNCTION IF EXISTS get_next_execution;
DELIMITER $$
