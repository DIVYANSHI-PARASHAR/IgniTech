import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// This reducer just prints the ouput form mapper as it is

public class hri extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text val : values) {
            result.set(val);
            context.write(key, result);
        }
    }
}