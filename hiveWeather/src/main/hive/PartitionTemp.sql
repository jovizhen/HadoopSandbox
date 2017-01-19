DROP TABLE IF EXISTS weather;

CREATE TABLE weather (year int, temp int, lat int, lon int)
PARTITIONED BY (dt STRING, country STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH'workstation/hiveinput/' INTO TABLE weather
PARTITION (dt='2017-1-18', country='US');

SELECT year, max(temp) WHERE country='US' AND temp!=-9999 AND temp!=-9999 GROUP BY year;
