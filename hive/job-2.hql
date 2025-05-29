DROP TABLE IF EXISTS price_tag;

CREATE EXTERNAL TABLE price_tag (
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
    city, 
    year,
    CASE 
        WHEN price < 20000 THEN 'basso'
        WHEN price BETWEEN 20000 AND 50000 THEN 'medio'
        ELSE 'alto'
    END AS fascia,
    COUNT(*) AS numero_macchine,
    AVG(daysonmarket) AS avg_daysonmarket
FROM price_tag
WHERE daysonmarket IS NOT NULL
GROUP BY city, year, 
    CASE 
        WHEN price < 20000 THEN 'basso'
        WHEN price BETWEEN 20000 AND 50000 THEN 'medio'
        ELSE 'alto'
    END
LIMIT 10;

DROP TABLE price_tag;