import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// this mapper emits zipcode with year as key and 1 as value
// this is for counting inspection for every year in each zipcode

public class YearZipcodeIMap extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text yearZipcode = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(",");

        // Ensure the record has enough data and the zipcode is valid
        if (columns.length >= 3) {
            String date = columns[0].trim();
            String zipcode = columns[2].trim();

            // Extract year from the date
            if (date.length() >= 4 && zipcode.matches("^\\d{5}$")) { // Ensure the date and zipcode are properly
                                                                     // formatted
                String year = date.substring(6, 10); // Extract the year assuming the format is DD/MM/YYYY
                String yearZipcodeKey = zipcode + " " + year; // Combine year and zipcode as a unique key

                yearZipcode.set(yearZipcodeKey);
                context.write(yearZipcode, one);
            } else {
                // Optionally handle invalid or unexpected entries
                Text invalidEntry = new Text("Invalid/Other");
                context.write(invalidEntry, one);
            }
        }
    }
}