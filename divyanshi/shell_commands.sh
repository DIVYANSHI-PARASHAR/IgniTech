###HYDRANTS DATA CLEANING

###(IN HYDRANT DIRECTORY)
#cd HYDRANT

#vi hmap.java
#vi hreduce.java
#vi hdrive.java

hadoop fs -mkdir hydrants
hdfs dfs -put hydrants.txt hydrants
javac -classpath `hadoop classpath` *.java
jar cvf hydrants.jar *.class
hadoop fs -rm -r /user/dp3635_nyu_edu/hydrants/output
hadoop jar zipareas.jar hdriver hydrants/hydrants.txt hydrants/output
hadoop fs -cat hydrants/output/part-r-00000

hadoop fs -getmerge hydrants/output /home/dp3635_nyu_edu/cleanHydrant.txt


###HYDRANTS DATA PROFILING ON RAW DATA

#vi profilehmap.java
#vi profilehreduce.java
#vi profilehdriver.java

hadoop fs -mkdir hydrantprofile
javac -classpath `hadoop classpath` *.java
jar cvf hydrantprofile.jar *.class
hadoop fs -rm -r /user/dp3635_nyu_edu/hydrantprofile/output
hadoop jar hydrantprofile.jar profilehdriver hydrants/hydrants.txt hydrantprofile/output
hadoop fs -ls hydrantprofile/output
hadoop fs -cat hydrantprofile/output/part-r-00000
hadoop fs -getmerge hydrantprofile/output /home/dp3635_nyu_edu/profiledrawhydrant.txt

###MATCHING- used geocoding api

###(IN MATCH DIRECTORY)

#cd MATCH

#vi hmi.java
#vi hri.java
#vi hd.java

hadoop fs -mkdir match
javac -classpath `hadoop classpath` *.java
jar cvf match.jar *.class
hadoop fs -rm -r /user/dp3635_nyu_edu/match/output
hadoop jar match.jar hd hydrants/output/part-r-00000 match/output
hadoop fs -ls match/output
hadoop fs -cat match/output/part-r-00000
hadoop fs -getmerge match/output /home/dp3635_nyu_edu/ziphydrant.txt

### DATA CLEANING ON ziphydrant.txt

#vi matchclean.java
#vi matchcleanMap.java
#vi matchcleanReduce.java

hadoop fs -mkdir matchclean
hdfs dfs -put ../ziphydrant.txt matchclean
javac -classpath `hadoop classpath` *.java
jar cvf matchclean.jar *.class
hadoop fs -rm -r /user/dp3635_nyu_edu/matchclean/output
hadoop jar matchclean.jar matchclean matchclean/ziphydrant.txt matchclean/output 
hadoop fs -ls matchclean/output
hadoop fs -cat matchclean/output/part-r-00000

hadoop fs -getmerge matchclean/output /home/dp3635_nyu_edu/cleanZipHydrant.txt

###PROFILING HYDRANTS ON FINAL CLEANED DATA

###BOROUGH-WISE

#vi boroughProfile.java
#vi boroughProfileMap.java
#vi boroughProfileReduce.java

hadoop fs -mkdir boroughHydrantprofile
hdfs dfs -put ../cleanZipHydrant.txt boroughHydrantprofile
javac -classpath `hadoop classpath` *.java
jar cvf boroughProfile.jar *.class
hadoop fs -rm -r /user/dp3635_nyu_edu/boroughHydrantprofile/output
hadoop jar boroughProfile.jar boroughProfile boroughHydrantprofile/cleanZipHydrant.txt boroughHydrantprofile/output
hadoop fs -ls boroughHydrantprofile/output
hadoop fs -cat boroughHydrantprofile/output/part-r-00000
hadoop fs -getmerge boroughHydrantprofile/output /home/dp3635_nyu_edu/borough-profiled-hydrant.txt

###ZIPCODE

#vi zipProfile.java
#vi zipProfileMap.java
#vi zipProfileReduce.java

hadoop fs -mkdir zipHydrantprofile
hdfs dfs -put ../cleanZipHydrant.txt zipHydrantprofile
javac -classpath `hadoop classpath` *.java
jar cvf zipProfile.jar *.class
hadoop fs -rm -r /user/dp3635_nyu_edu/zipHydrantprofile/output
hadoop jar zipProfile.jar zipProfile zipHydrantprofile/cleanZipHydrant.txt zipHydrantprofile/output
hadoop fs -ls zipHydrantprofile/output
hadoop fs -cat zipHydrantprofile/output/part-r-00000
hadoop fs -getmerge zipHydrantprofile/output /home/dp3635_nyu_edu/zip-profiled-hydrant.txt


###INSPECTION CLEANING

#vi IMap.java
#vi IReduce.java
#vi Inspection

hadoop fs -mkdir insp
hdfs dfs -put inspection.txt insp
hadoop fs -rm -r /user/dp3635_nyu_edu/insp/output
javac -classpath `hadoop classpath` *.java
jar cvf inspection.jar *.class
hadoop jar inspection.jar Inspection insp/inspection.txt insp/output
hadoop fs -ls insp/output
hadoop fs -cat insp/output/part-r-00000

hadoop fs -getmerge insp/output /home/dp3635_nyu_edu/cleanInspection.txt


###INSPECTION PROFILING OF RAW DATA

#vi boroProfileIDriver.java
#vi boroProfileIMap.java
#vi boroProfileIReduce.java

hadoop fs -mkdir inspprofileraw
hadoop fs -rm -r /user/dp3635_nyu_edu/inspprofileraw/output
javac -classpath `hadoop classpath` *.java
jar cvf inspprofileraw.jar *.class
hadoop jar inspprofileraw.jar boroProfileIDriver insp/inspection.txt inspprofileraw/output
hadoop fs -ls inspprofileraw/output
hadoop fs -cat inspprofileraw/output/part-r-00000

###INSPECTION PROFILING OF BOROUGH WISE CLEANED DATA
### (same files as that of raw cleaning, just directory changed)

hadoop fs -mkdir inspprofile
hadoop fs -cp insp/output/part-r-00000 inspprofile/cleanInspection.txt
hdfs dfs -put cleanInspection.txt inspprofile
hadoop fs -rm -r /user/dp3635_nyu_edu/inspprofile/output
javac -classpath `hadoop classpath` *.java
jar cvf inspprofile.jar *.class
hadoop jar inspprofile.jar boroProfileIDriver inspprofile/cleanInspection.txt inspprofile/output
hadoop fs -ls inspprofile/output
hadoop fs -cat inspprofile/output/part-r-00000
hadoop fs -getmerge inspprofile/output /home/dp3635_nyu_edu/boroProfInsp.txt

# For yearwise just change mapper YearBoroughIMap.java, use same commands and store results using:
hadoop fs -getmerge inspprofile/output /home/dp3635_nyu_edu/yearboroProfInsp.txt

###INSPECTION PROFILING OF ZIPCODE WISE CLEANED DATA

#vi zipProfileIMap.java
#vi zipProfileIReduce.java
#vi zipProfileIDriver.java

hadoop fs -mkdir zipProfInsp
hadoop fs -cp insp/output/part-r-00000 zipProfInsp/cleanInsp.txt
hdfs dfs -put cleanInspection.txt zipProfInsp
hadoop fs -rm -r /user/dp3635_nyu_edu/zipProfInsp/output
javac -classpath `hadoop classpath` *.java
jar cvf zipProfInsp.jar *.class
hadoop jar zipProfInsp.jar zipProfileIDriver zipProfInsp/cleanInsp.txt zipProfInsp/output
hadoop fs -ls zipProfInsp/output
hadoop fs -cat zipProfInsp/output/part-r-00000
hadoop fs -getmerge zipProfInsp/output /home/dp3635_nyu_edu/zipProfInsp.txt

# For yearwise just change mapper YearZipcodeIMap.java, use same commands and store results using:
hadoop fs -getmerge zipProfInsp/output /home/dp3635_nyu_edu/yearzipProfInsp.txt
