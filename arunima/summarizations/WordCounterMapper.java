import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;

public class WordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private int table_column_index;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
                table_column_index = Integer.parseInt(context.getConfiguration().get("table_column_index"));
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columnValues = value.toString().split(",");
			
			// uncomment this line if WordCounter has to be performed on the entire dataset
            // if(columnValues.length!=29 || columnValues[0].equals("STARFIRE_INCIDENT_ID")) return;

            context.write(new Text(columnValues[table_column_index]), new IntWritable(1));

        }
}