import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.MapWritable;

import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.StringTokenizer;

public class NumericalSummarizationMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {

        private int table_column_index;
        private Map<String, String> hashMap = new HashMap<>();
        private final String MIN = "min", MAX = "max", SUM = "sum", COUNT = "count";

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
                table_column_index = Integer.parseInt(context.getConfiguration().get("table_column_index"));
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] columnValues = value.toString().split(",");
	
		// UNCOMMENT this line if summarization is being performed on raw dataset
        //if (columnValues.length != 29) {
        //              return;
        //}
        if(columnValues[table_column_index].length()==0 || columnValues[table_column_index]==null) return;
                updateMap(Double.parseDouble(columnValues[table_column_index]));
        }

        private void updateMap(double val) {
                hashMap.put(SUM, Double.toString(Double.parseDouble(hashMap.getOrDefault(SUM, "0")) + val));
                hashMap.put(COUNT, Double.toString(Double.parseDouble(hashMap.getOrDefault(COUNT, "0")) + 1.0));
                hashMap.put(MIN, Double.toString(Math.min(val, Double.parseDouble(hashMap.getOrDefault(MIN, "9999999.0")))));
                hashMap.put(MAX, Double.toString(Math.max(val, Double.parseDouble(hashMap.getOrDefault(MAX, "0.0")))));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
                MapWritable ouptutMap = new MapWritable();
                for (String key : hashMap.keySet()) {
                        ouptutMap.put(new Text(key), new Text(hashMap.get(key)));
                }
                context.write(NullWritable.get(), ouptutMap);
        }
}