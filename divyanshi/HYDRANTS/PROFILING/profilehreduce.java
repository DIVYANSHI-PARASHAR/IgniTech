import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class profilehreduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        // Summing up the counts for each borough
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Setting the result count
        result.set(sum);

        // Emitting the borough and its corresponding count
        context.write(key, result);
    }
}