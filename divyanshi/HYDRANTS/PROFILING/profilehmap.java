import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class profilehmap extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text borough = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Splitting the input line by comma
        String[] fields = value.toString().split(",");

        // Extracting the borough from the input line
        String boroughName = fields[1].trim();

        // Emitting the borough name as the key and 1 as the value
        borough.set(boroughName);
        context.write(borough, one);
    }

}