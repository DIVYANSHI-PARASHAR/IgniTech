import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// This mapper is for calculaitng the number of hydrants per zip code
// It emits the zipcode as key and 1 as value
// The strudture is exactly same as that for calculating hydrants per borough

public class zipProfileMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text zip = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Splitting the input line by comma
        String[] fields = value.toString().split(",");

        // Extracting the zipcode from the input line
        String zipcode = fields[1];

        // Emitting the borough name as the key and 1 as the value
        zip.set(zipcode);
        context.write(zip, one);
    }
}