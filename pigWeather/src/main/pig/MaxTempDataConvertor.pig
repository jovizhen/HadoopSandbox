REGISTER pigWeather-0.0.1.jar;
records = LOAD '/Users/jing/workstation/piginput/'
USING com.jovi.pig.CutLoadFunc('year, temp, lat, lon')
  AS (year:chararray, temperature:int, lat:biginteger, long:biginteger);
filtered_records = FILTER records BY temperature != -9999 AND
temperature != -8888;
STORE filtered_records INTO 'hiveinput';