import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;

public class InvalidEntriesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final String[] columnNames = {"STARFIRE_INCIDENT_ID","INCIDENT_DATETIME","ALARM_BOX_BOROUGH",
        "ALARM_BOX_NUMBER","ALARM_BOX_LOCATION","INCIDENT_BOROUGH","ZIPCODE",
        "POLICEPRECINCT","CITYCOUNCILDISTRICT","COMMUNITYDISTRICT","COMMUNITYSCHOOLDISTRICT",
        "CONGRESSIONALDISTRICT","ALARM_SOURCE_DESCRIPTION_TX","ALARM_LEVEL_INDEX_DESCRIPTION",
        "HIGHEST_ALARM_LEVEL","INCIDENT_CLASSIFICATION","INCIDENT_CLASSIFICATION_GROUP",
        "DISPATCH_RESPONSE_SECONDS_QY","FIRST_ASSIGNMENT_DATETIME","FIRST_ACTIVATION_DATETIME",
        "FIRST_ON_SCENE_DATETIME","INCIDENT_CLOSE_DATETIME","VALID_DISPATCH_RSPNS_TIME_INDC",
        "VALID_INCIDENT_RSPNS_TIME_INDC","INCIDENT_RESPONSE_SECONDS_QY","INCIDENT_TRAVEL_TM_SECONDS_QY",
        "ENGINES_ASSIGNED_QUANTITY","LADDERS_ASSIGNED_QUANTITY","OTHER_UNITS_ASSIGNED_QUANTITY"};

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] columnValues = value.toString().split(",");

                // Checking if row conforms to the schema
                if(columnValues.length!=29) {
                        context.write(new Text("Rows with incorrect schema = "), new IntWritable(1));
                        return;
                }

                // Checking missing entries for each column
                int counter=0;
                for(int i=0;i<29;i++){
                        if(columnValues[i]==null || columnValues[i].length()==0){
                                context.write(new Text(columnNames[i]), new IntWritable(1));
                                return;
                        }
                }
               
                return;
        }
}