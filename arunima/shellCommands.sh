#!/bin/bash

### Write dataset on HDFS
hdfs dfs -mkdir Fire_Incident_original
hdfs dfs -put ./Fire_Incident.csv Fire_Incident_original

## Data Cleaning Job
javac -classpath `hadoop classpath` DataCleaning*.java
jar cvf fire1.jar DataCleaning*.class
hadoop fs -mkdir fire_outputs
hadoop jar fire1.jar DataCleaning Fire_Incident_original/Fire_Incident.csv fire_outputs/output_1

### Profiling Jobs

## Invalid Entry Profiling Job
javac -classpath `hadoop classpath` InvalidEntries*.java
jar cvf invalid_entries.jar InvalidEntries*.class
hadoop jar invalid_entries.jar InvalidEntries Fire_Incident_original/Fire_Incident.csv fire_outputs/output_invalid_entries

## Word Counter Profiling Job
javac -classpath `hadoop classpath` WordCounter*.java
jar cvf wc.jar WordCounter*.class
hadoop jar wc.jar WordCounter fire_outputs/output_1/part-r-00000 fire_outputs/output_1_borough 0
hadoop jar wc.jar WordCounter fire_outputs/output_1/part-r-00000 fire_outputs/output_1_inc_class 4
hadoop jar wc.jar WordCounter fire_outputs/output_1/part-r-00000 fire_outputs/output_1_inc_grp_class 3

## Numerical Summarization Profiling Job
javac -classpath `hadoop classpath` NumericalSummarization*.java
jar cvf numerical.jar NumericalSummarization*.class
hadoop jar numerical.jar NumericalSummarization fire_outputs/output_1/part-r-00000 fire_outputs/output_1_numer 7
