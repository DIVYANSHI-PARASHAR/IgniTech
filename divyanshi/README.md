# IGNITECH : Fighting Fire With Data

## Data Cleaning and Profiling on Fire Hydrant and inspection Data
By Divyanshi Parashar (dp3635@nyu.edu)

### Overview
This phase includes utilizing Hadoop's MapReduce framework to perform ingestion, profiling, preprocessing and cleaning on the NYC Fire Hydrants and Fire Inspections dataset. 

### Fire Inspection: 
This dataset captures historical information on inspections conducted by the Bureau of Fire Prevention, focusing on permits and inspection details categorized by account holders from 2014 to 2019. The data is provided by the Fire Department of New York (FDNY).
Dataset size - 72.3 MB

### Hydrant Data: 
This dataset captures the location of fire hydrants across New York in 2023. The data is provided by the Department of Environmental Protection(DEP) Dataset Size - 12.1 MB

### PROJECT FOLDERS

- **HYDRANTS:** Consists of cleaning and profiling folders for hydrant data.

    CLEANING

    The files for cleaning are: 
    
    `hmap.java`, `hreduce.java`, `hdriver.java`.
    
    PROFILING

    The files for profiling are: 
    
    `profilehmap.java`, `profilehreduce.java`, `profilehdriver.java`.

- **ADD_ZIPCODE** Consists of adding zipcodes, further cleaning and profiling folders for hydrant data.

    ADDING ZIPCODES

    The files for adding zipcodes are: 
    
    `hmi.java`, `hri.java`, `hd.java`.
    
    Add you API key to fetch zipcodes here.
    
    CLEANING

    The files for cleaning hydrant data after addition/matching zipcodes are: 
    
    `matchcleanMap.java`, `matchcleanReduce.java`, `matchclean.java` .

    PROFILING

    The files for profiling borough-wise are: 
    
    `boroughProfileMap.java`, `boroughProfileReduce.java`, `boroughProfile.java` .

    The files for profiling zip-code-wise are: 
    
    `zipProfileMap.java`, `zipProfileReduce.java`, `zipProfile.java` .

- **INSPECTION** Consistes of cleaning and profiling folders for inspection data.

    CLEANING

    The files for cleaning are: 

    `IMap.java`, `IReduce.java`, `Inspection.java`.

    PROFILING

    The files for profiling borough-wise are: 
    
    `boroProfileIMap.java`, `boroProfileIReduce.java`, `boroProfileIDriver.java`. 
    
    For yearwise: `YearBoroughIMap.java`. Reducer and Driver remains the same as above. Don't forget to change mapper name in dirver for this.

    The files for profiling zip-code-wise are: 
    
    `zipProfileIMap.java`, `zipProfileIReduce.java`, `zipIProfileIDriver.java`. 
    
    For yearwise: `YearZipcodeIMap.java`. Reducer and Driver remains the same as above. Don't forget to change mapper name in dirver for this.

### Shell Commands
Added in shell Commands.sh
