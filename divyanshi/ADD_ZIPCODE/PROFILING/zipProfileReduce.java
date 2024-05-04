import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// This reducer sums up the hydrant counts for each zipcode key

public class zipProfileReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        // Summing up the counts for each zipcode
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Setting the result count
        result.set(sum);

        // Emitting the zipcode and its corresponding count
        context.write(key, result);
    }
}