DROP TABLE IF EXISTS model_statistics;

CREATE EXTERNAL TABLE model_statistics (
    city STRING,
    daysonmarket INT,
    description STRING,
    engine_displacement FLOAT,
    horsepower FLOAT,
    make_name STRING,
    model_name STRING,
    price FLOAT,
    year INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/rainer/data';

SELECT 
    make_name,
    model_name,
    COUNT(*) as num_cars,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price) as avg_price,
    COLLECT_SET(year) as years_list
FROM model_statistics
GROUP BY make_name, model_name
LIMIT 10;

DROP TABLE model_statistics;