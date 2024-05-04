import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

public class DataCleaningReducer extends Reducer<NullWritable, Text, NullWritable, Text>
{
    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

                for (Text val : values) {
                    context.write(NullWritable.get(), val);
                  }
       
   }
}
