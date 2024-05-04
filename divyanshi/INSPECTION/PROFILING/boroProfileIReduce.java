import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// theis reducer sums up thecount of inspections per borough

public class boroProfileIReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int totalCount = 0;
        for (IntWritable value : values) {
            totalCount += value.get();
        }
        context.write(key, new IntWritable(totalCount));
    }
}