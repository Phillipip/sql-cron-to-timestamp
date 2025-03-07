DROP FUNCTION IF EXISTS cron_to_json;
DELIMITER $$
CREATE FUNCTION cron_to_json(cron_str TEXT)
RETURNS JSON
DETERMINISTIC
BEGIN
  RETURN JSON_OBJECT(
    'second', JSON_EXTRACT(process_cron_field(SUBSTRING_INDEX(cron_str, ' ', 1), 0, 59), '$'),
    'minute', JSON_EXTRACT(process_cron_field(SUBSTRING_INDEX(SUBSTRING_INDEX(cron_str, ' ', 2), ' ', -1), 0, 59), '$'),
    'hour',   JSON_EXTRACT(process_cron_field(SUBSTRING_INDEX(SUBSTRING_INDEX(cron_str, ' ', 3), ' ', -1), 0, 23), '$'),
    'day_of_month', JSON_EXTRACT(process_cron_field(SUBSTRING_INDEX(SUBSTRING_INDEX(cron_str, ' ', 4), ' ', -1), 1, 31), '$'),
    'month',  JSON_EXTRACT(process_cron_field(SUBSTRING_INDEX(SUBSTRING_INDEX(cron_str, ' ', 5), ' ', -1), 1, 12), '$'),
    'day_of_week', JSON_EXTRACT(process_cron_field(SUBSTRING_INDEX(SUBSTRING_INDEX(cron_str, ' ', 6), ' ', -1), 0, 6), '$')
  );
END$$
DELIMITER ;
