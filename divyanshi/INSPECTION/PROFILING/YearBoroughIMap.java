import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// this mapper emits borough with year as key and 1 as value
// this is for counting inspection for every year in each borough

public class YearBoroughIMap extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text yearBorough = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(",");

        // Check that we have the expected number of columns to include a date and borough
        if (columns.length > 3) {
            String date = columns[0].trim(); // date is in the first column
            String boroughCode = columns[columns.length - 1].trim(); // Get the last column for the borough code

            // Validate borough code and extract year from date
            if (boroughCode.matches("^(BRONX|MANHATTAN|RICHMOND / STATEN ISLAND|BROOKLYN|QUEENS)$")
                    && date.length() >= 10) {
                String year = date.substring(6, 10); // Extract the year from date format is MM/DD/YYYY
                String yearBoroughKey = boroughCode + " " + year; // Form a key by combining year and borough

                yearBorough.set(yearBoroughKey);
                context.write(yearBorough, one);

            } else {
                // Optionally handle invalid or unexpected entries
                Text invalidEntry = new Text("Invalid/Other");
                context.write(invalidEntry, one);
            }
        }
    }
}