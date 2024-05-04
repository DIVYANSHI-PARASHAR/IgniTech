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
``` select year(incident_datetime), borough, avg(travel_time_seconds) from fire_incident group by borough, year(incident_datetime) order by borough, year(inc
ident_datetime);```

5. Incidents average response_time per borough across years
``` select year(incident_datetime), borough, avg(response_time_seconds) from fire_incident group by borough, year(incident_datetime) order by borough, year(incident_datetime);```

6. Count of structural and non-structural fires across years
``` select incident_class_group, year(incident_datetime), count(*) from fire_incident group by incident_class_group, year(incident_datetime); ```

7. Count number of incident for each incident class(Private Dwelling Fire, School Fire, Automobile Fire, etc)
```select incident_class_group, incident_class, count(*) from fire_incident group by incident_class, incident_class_group;```


