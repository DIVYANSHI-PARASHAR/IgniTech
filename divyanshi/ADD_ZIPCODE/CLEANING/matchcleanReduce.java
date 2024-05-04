import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

public class matchcleanReduce extends Reducer<NullWritable, Text, Text, NullWritable> {

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(new Text(value.toString()), NullWritable.get());
        }
    }
}