# IGNITECH : Fighting Fire With Data

## Data Cleaning and Profiling on Fire Incidents Data  
By Arunima Mitra (am13018@nyu.edu)

### Overview

This phase includes utilizing Hadoop's MapReduce framework to perform ingestion, profiling, preprocessing and cleaning on the NYC Fire Incidents dataset. This dataset captures detailed information about incidents handled by the FDNY and includes fire, medical and non-medical emergencies.

### Project Structure

- **DataCleaning.java:** Data cleaning job for filtering and preprocessing the data.
- **DataCleaningMapper.java:** Mapper class for the data cleaning job.
- **DataCleaningReducer.java:** Reducer class for the data cleaning job.

- **InvalidEntries.java:** Data profiling job for counting number of invalid entries.
- **InvalidEntriesMapper.java:** Mapper class for the data profiling job
- **InvalidEntriesReducer.java:** Reducer class for the data profiling job

- **WordCounter.java:** Java program for counting number of words - used for profiling data column-wise.
- **WordCounterMapper.java:** Mapper class for the Word Counter job.
- **WordCounterReducer.java:** Reducer class for the Word Counter job.

- **NumericalSummarization.java:** Java program for calculating min,max,mean - used for profiling data column-wise
- **NumericalSummarizationMapper.java:** Mapper class for the summarization job.
- **NumericalSummarizationReducer.java:** Reducer class for the summarization job.

### Shell Commands

Attached [ shellCommands.sh ] 