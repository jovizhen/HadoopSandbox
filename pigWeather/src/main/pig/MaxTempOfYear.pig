REGISTER pigWeather-0.0.1.jar;
records = LOAD '/Users/jing/workstation/piginput/'
USING com.jovi.pig.CutLoadFunc('year, temp')
  AS (year:chararray, temperature:int);
DUMP records;
filtered_records = FILTER records BY temperature != -9999 AND
temperature != -8888;
DUMP filtered_records;
grouped_records = GROUP filtered_records BY year;
DUMP grouped_records;
max_temp = FOREACH grouped_records GENERATE group,
  MAX(filtered_records.temperature);
-- ^^ max_temp_max_temp
-- vv max_temp_result
DUMP max_temp;