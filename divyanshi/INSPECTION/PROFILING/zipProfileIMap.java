package INSPECTION.PROFILING;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// This mapper fetches zip codes and calculates inspection counts per zipcode with the help of reducer
// It emits zipcode as key and 1 as value

public class zipProfileIMap extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text zipcode = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(",");

        // Check if the data is complete and the zipcode field exists
        if (columns.length > 2) {
            String zip = columns[2].trim(); // zipcode is the third last element

            // Ensure the zipcode is exactly 5 characters long to meet your criteria
            if (zip.matches("^\\d{5}$")) {
                zipcode.set(zip);
                context.write(zipcode, one);
            } else {
                // Optionally handle invalid or unexpected zipcodes
                Text invalidZip = new Text("Invalid/Other");
                context.write(invalidZip, one);
            }
        }
    }
}
