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

#### Borough-wise count
``` SELECT borough, COUNT(*) AS incidents_per_borough FROM fire_incident GROUP BY borough; ```

#### Borough-wise distribution across years
``` 
SELECT borough, EXTRACT(YEAR FROM FROM_UNIXTIME(UNIX_TIMESTAMP(incident_datetime, 'MM/dd/yyyy hh:mm:ss'))) AS incident_year, COUNT(*) AS count_per_year
FROM fire_incident
GROUP BY borough, EXTRACT(YEAR FROM FROM_UNIXTIME(UNIX_TIMESTAMP(incident_datetime, 'MM/dd/yyyy hh:mm:ss')))
ORDER BY borough, incident_year;
```

