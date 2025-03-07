DROP FUNCTION IF EXISTS match_value;
DELIMITER $$
CREATE FUNCTION match_value(
  candidate INT,
  op VARCHAR(10),
  eq_val INT,
  between_start INT,
  between_end INT,
  in_list TEXT
) RETURNS BOOLEAN
DETERMINISTIC
BEGIN
  IF op = 'ALL' THEN
    RETURN TRUE;
  ELSEIF op = '=' THEN
    RETURN candidate = eq_val;
  ELSEIF op = 'BETWEEN' THEN
    RETURN candidate BETWEEN between_start AND between_end;
  ELSEIF op = 'IN' THEN
    RETURN FIND_IN_SET(candidate, in_list) > 0;
  ELSE
    RETURN FALSE;
  END IF;
END$$
DELIMITER ;
