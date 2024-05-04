import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// This mapper searches for borough codes and calclated isnpection counts with the help of reducer
// It emits borough as key and 1 as value

public class boroProfileIMap extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text borough = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(",");

        // Borough codes to search for
        String[] boroughCodes = { "BRONX", "MANHATTAN", "BROOKLYN", "QUEENS", "RICHMOND / STATEN ISLAND" };

        // Check each column for borough codes
        for (String boroughCode : boroughCodes) {
            for (String column : columns) {
                if (column.contains(boroughCode)) {
                    borough.set(boroughCode);
                    context.write(borough, one);
                    return; // Stop searching after finding the borough code
                }
            }
        }
    }
}