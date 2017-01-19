DROP TABLE IF EXISTS weather;

CREATE TABLE weather (year int, temp int, lat int, lon int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH'input/weather' OVERWRITE INTO TABLE weather;

SELECT year , MAX(temp) FROM weather WHERE temp != -9999 AND temp!= -8888 GROUP BY year;