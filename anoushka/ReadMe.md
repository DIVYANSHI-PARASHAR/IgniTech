### Weather Data
- Weather Raw Data Profiler <br>
`hadoop jar WeatherProfiler.jar WeatherProfiler weather_profile/NYC_Weather_data.txt weather_profile/output`
- Data Profiler commands <br>
`hadoop jar yearTemp.jar YearTemp YearTemp_profile/NYC_Weather_data.txt YearTemp_profile/output` <br>
`hadoop jar monthTemp.jar MonthTemp MonthTemp_profile/NYC_Weather_data.txt MonthTemp_profile/output` <br>
- Data cleaning <br>
`hadoop jar WeatherFilter.jar WeatherFilter WeatherFilter/NYC_Weather_data.txt WeatherFilter/output`

### Decennial Data
- Command to clean run data cleaning map reduce job and aggreagte population and housing units data <br>
`hadoop jar DecennialCombiner.jar DecennialCombiner DecennialCombiner/population.txt DecennialCombiner/housing.txt DecennialCombiner/output`

### Mean Income Data Cleaning
`hadoop jar MeanIncome.jar MeanIncome MeanIncome MeanIncome/output`

### Mean Income Data Profiler
`hadoop jar meanProfiler.jar MeanProfiler meanProfiler/alldata_clean.txt meanProfiler/output`

### Median Income Data Cleaning
`hadoop jar MedianIncome.jar MedianIncome MedianIncome MediaanIncome/output`

### Age Gender Data Cleaning
`hadoop jar AgeGender.jar AgeGender AgeGender AgeGender/output`

### Data Combiner map reduce for mean,median and age gender data
`hadoop jar DataCombiner.jar DataCombiner DataCombiner/meanIncome_clean.txt DataCombiner/medianIncome_clean.txt DataCombiner/ageGender_clean.txt DataCombiner/output`



  


  
