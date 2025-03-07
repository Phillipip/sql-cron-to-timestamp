DROP FUNCTION IF EXISTS cron_to_json;
DELIMITER $$
CREATE FUNCTION cron_to_json(cron_str TEXT)
RETURNS JSON
DETERMINISTIC
BEGIN
    DECLARE sec TEXT;
    DECLARE min TEXT;
    DECLARE hr  TEXT;
    DECLARE dom TEXT;
    DECLARE mon TEXT;
    DECLARE dow TEXT;
    
    SET sec = SUBSTRING_INDEX(cron_str, ' ', 1);
    SET min = SUBSTRING_INDEX(SUBSTRING_INDEX(cron_str, ' ', 2), ' ', -1);
    SET hr  = SUBSTRING_INDEX(SUBSTRING_INDEX(cron_str, ' ', 3), ' ', -1);
    SET dom = SUBSTRING_INDEX(SUBSTRING_INDEX(cron_str, ' ', 4), ' ', -1);
    SET mon = SUBSTRING_INDEX(SUBSTRING_INDEX(cron_str, ' ', 5), ' ', -1);
    SET dow = SUBSTRING_INDEX(SUBSTRING_INDEX(cron_str, ' ', 6), ' ', -1);
    
    RETURN JSON_OBJECT(
         'second', JSON_EXTRACT(process_cron_field(sec, 0, 59), '$'),
         'minute', JSON_EXTRACT(process_cron_field(min, 0, 59), '$'),
         'hour',   JSON_EXTRACT(process_cron_field(hr, 0, 23), '$'),
         'day_of_month', JSON_EXTRACT(process_cron_field(dom, 1, 31), '$'),
         'month',  JSON_EXTRACT(process_cron_field(mon, 1, 12), '$'),
         'day_of_week', JSON_EXTRACT(process_cron_field(dow, 0, 6), '$')
    );
END$$
DELIMITER ;
