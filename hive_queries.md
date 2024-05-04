## Creating tables

```
CREATE EXTERNAL TABLE fire_incident
(BOROUGH string,
ZIPCODE int, 
ALARM_LOCATION string,
INCIDENT_CLASS_GROUP string,
INCIDENT_CLASS string,
INCIDENT_DATETIME timestamp, 
INCIDENT_CLOSE_TIME timestamp,
RESPONSE_TIME_SECONDS int,
TRAVEL_TIME_SECONDS int,
ALARM_BOX_NUMBER int, 
POLICE_PRECINCT int,  
ALARM_SRC string, 
ENGINES_ASSIGNED int, 
LADDERS_ASSIGNED int,
INCIDENT_ROUNDED_DATETIME string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/am13018_nyu_edu/fire_outputs/output_1';
```
```
CREATE EXTERNAL TABLE weather (
    capture_time STRING,
    temperature FLOAT,
    precipitation FLOAT,
    rain FLOAT,
    cloudcover_percent FLOAT,
    cloudcover_low_percent FLOAT,
    cloudcover_mid_percent FLOAT,
    cloudcover_high_percent FLOAT,
    windspeed FLOAT,
    winddirection FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/ag8733_nyu_edu/project_weather/output';
```
```
CREATE EXTERNAL TABLE census (
    year INT,
    zipcode INT,
   borough STRING,
    meanIncome DOUBLE,
    meanIncome_latino DOUBLE,
    meanIncome_white DOUBLE,
    meanIncome_black DOUBLE,
    meanIncome_asian DOUBLE,
    medianIncome DOUBLE,
    medianIncome_latino DOUBLE,
    medianIncome_white DOUBLE,
    medianIncome_black DOUBLE,
    medianIncome_asian DOUBLE,
    tot_pop INT,
    tot_children INT,
    tot_oldies INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/ag8733_nyu_edu/Census_bor/output';
```
```
CREATE EXTERNAL TABLE decennial (
    zipcode INT,
    population INT,
    housing_units INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/ag8733_nyu_edu/project_decennial/output';
```
## Presto queries for analyzing datasets

1. Borough-wise count of fire incidents
``` SELECT borough, COUNT(*) AS incidents_per_borough FROM fire_incident GROUP BY borough; ```

2. Borough-wise distribution of fire incidents across years
``` 
SELECT borough, EXTRACT(YEAR FROM FROM_UNIXTIME(UNIX_TIMESTAMP(incident_datetime, 'MM/dd/yyyy hh:mm:ss'))) AS incident_year, COUNT(*) AS count_per_year
FROM fire_incident
GROUP BY borough, EXTRACT(YEAR FROM FROM_UNIXTIME(UNIX_TIMESTAMP(incident_datetime, 'MM/dd/yyyy hh:mm:ss')))
ORDER BY borough, incident_year;
```

3. Incident distribution across police precincts and boroughs
```
SELECT borough, police_precinct, COUNT(*) as incident_count FROM fire_incident GROUP BY police_precinct, borough order by borough desc;
```

4. Incidents average travel_time per borough across years
``` select year(incident_datetime), borough, avg(travel_time_seconds) from fire_incident group by borough, year(incident_datetime) order by borough, year(incident_datetime);```

5. Incidents average response_time per borough across years
``` select year(incident_datetime), borough, avg(response_time_seconds) from fire_incident group by borough, year(incident_datetime) order by borough, year(incident_datetime);```

6. Count of structural and non-structural fires across years
``` select incident_class_group, year(incident_datetime), count(*) from fire_incident group by incident_class_group, year(incident_datetime); ```

7. Count number of incident for each incident class(Private Dwelling Fire, School Fire, Automobile Fire, etc)
```select incident_class_group, incident_class, count(*) from fire_incident group by incident_class, incident_class_group;```

8. Count number of incidents for every temperature range - defined four temp ranges (<0, 0-15, 15-30, 30+)

    ``` 

    SELECT
        temperature_ranges.temperature_range,
        COUNT() AS incident_count,
        temperature_frequency 
    FROM
        ( SELECT
            CASE 
                WHEN temperature < 0 THEN '<0' 
                WHEN temperature BETWEEN 0 AND 15 THEN '0-15' 
                WHEN temperature BETWEEN 15 AND 30 THEN '15-30' 
                ELSE '30 ' 
            END AS temperature_range,
            CAST(temperature AS DOUBLE) AS temperature,
            capture_time 
        FROM
            weather ) AS temperature_ranges 
    INNER JOIN
        fire_incident 
            ON fire_incident.incident_rounded_datetime = temperature_ranges.capture_time 
    LEFT JOIN
        (
            SELECT
                CASE 
                    WHEN temperature < 0 THEN '<0' 
                    WHEN temperature BETWEEN 0 AND 15 THEN '0-15' 
                    WHEN temperature BETWEEN 15 AND 30 THEN '15-30' 
                    ELSE '30 ' 
                END AS temperature_range,
                COUNT() AS temperature_frequency 
            FROM
                weather 
            GROUP BY
                CASE 
                    WHEN temperature < 0 THEN '<0' 
                    WHEN temperature BETWEEN 0 AND 15 THEN '0-15' 
                    WHEN temperature BETWEEN 15 AND 30 THEN '15-30' 
                    ELSE '30 ' 
                END 
        ) AS temperature_frequency_counts 
            ON temperature_ranges.temperature_range = temperature_frequency_counts.temperature_range 
    GROUP BY
        temperature_ranges.temperature_range,
        temperature_frequency 
    ORDER BY
        temperature_ranges.temperature_range;
    ```

9. Count number of incidents for every precipitation range - defined four precipitation ranges (No rain, light showers, moderate showers, and heavy showers)

```
    SELECT
        precipitation_ranges.precipitation_range,
        COUNT() AS incident_count,
        precipitation_frequency 
    FROM
        ( SELECT
            CASE 
                WHEN precipitation =0.0 THEN 'No rain' 
                WHEN precipitation >= 0.1 
                AND precipitation<=0.9 THEN 'Light Showers' 
                WHEN precipitation BETWEEN 1.0 AND 10 THEN 'Moderate Showers' 
                ELSE 'Heavy Showers' 
            END AS precipitation_range,
            CAST(precipitation AS DOUBLE) AS precipitation,
            capture_time 
        FROM
            weather ) AS precipitation_ranges 
    INNER JOIN
        fire_incident 
            ON fire_incident.incident_rounded_datetime = precipitation_ranges.capture_time 
    LEFT JOIN
        (
            SELECT
                CASE 
                    WHEN precipitation =0.0 THEN 'No rain' 
                    WHEN precipitation >= 0.1 
                    AND precipitation<=0.9 THEN 'Light Showers' 
                    WHEN precipitation BETWEEN 1.0 AND 10 THEN 'Moderate Showers' 
                    ELSE 'Heavy Showers' 
                END AS precipitation_range,
                COUNT() AS precipitation_frequency 
            FROM
                weather 
            GROUP BY
                CASE 
                    WHEN precipitation =0.0 THEN 'No rain' 
                    WHEN precipitation >= 0.1 
                    AND precipitation<=0.9 THEN 'Light Showers' 
                    WHEN precipitation BETWEEN 1.0 AND 10 THEN 'Moderate Showers' 
                    ELSE 'Heavy Showers' 
                END 
        ) AS precipitation_frequency_counts 
            ON precipitation_ranges.precipitation_range = precipitation_frequency_counts.precipitation_range 
    GROUP BY
        precipitation_ranges.precipitation_range,
        precipitation_frequency 
    ORDER BY
        precipitation_ranges.precipitation_range;
```

10. Count number of incidents for all windspeed ranges - defined five windspeed ranges (<10, 10-20, 20-30, 30-40, 40+)

```
    SELECT
        windspeed_ranges.windspeed_range,
        COUNT() AS incident_count,
        windspeed_frequency 
    FROM
        ( SELECT
            CASE 
                WHEN windspeed BETWEEN 0.0 AND 10.0THEN '<10' 
                WHEN windspeed BETWEEN 10 AND 20 THEN '10-20' 
                WHEN windspeed BETWEEN 20 AND 30 THEN '20-30' 
                WHEN windspeed BETWEEN 30 AND 40 THEN '30-40' 
                ELSE '40 ' 
            END AS windspeed_range,
            CAST(windspeed AS DOUBLE) AS windspeed,
            capture_time 
        FROM
            weather ) AS windspeed_ranges 
    INNER JOIN
        fire_incident 
            ON fire_incident.incident_rounded_datetime = windspeed_ranges.capture_time 
    LEFT JOIN
        (
            SELECT
                CASE 
                    WHEN windspeed BETWEEN 0.0 AND 10.0THEN '<10' 
                    WHEN windspeed BETWEEN 10 AND 20 THEN '10-20' 
                    WHEN windspeed BETWEEN 20 AND 30 THEN '20-30' 
                    WHEN windspeed BETWEEN 30 AND 40 THEN '30-40' 
                    ELSE '40 ' 
                END AS windspeed_range,
                COUNT() AS windspeed_frequency 
            FROM
                weather 
            GROUP BY
                CASE 
                    WHEN windspeed BETWEEN 0.0 AND 10.0THEN '<10' 
                    WHEN windspeed BETWEEN 10 AND 20 THEN '10-20' 
                    WHEN windspeed BETWEEN 20 AND 30 THEN '20-30' 
                    WHEN windspeed BETWEEN 30 AND 40 THEN '30-40' 
                    ELSE '40 ' 
                END 
        ) AS windspeed_frequency_counts 
            ON windspeed_ranges.windspeed_range = windspeed_frequency_counts.windspeed_range 
    GROUP BY
        windspeed_ranges.windspeed_range,
        windspeed_frequency 
    ORDER BY
        windspeed_ranges.windspeed_range;
```
