import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

// This mapper is for cleaning teh hydrant data again after adding the zipcodes
// I have changed the boroughnames here to match with other data sets in our project

public class matchcleanMap extends Mapper<LongWritable, Text, NullWritable, Text> {
    private Text output = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Regex to split the line right at the start of "POINT" to separate borough
        // from coordinates and zipcode
        String[] parts = value.toString().split("\\s+POINT");

        if (parts.length == 2) {
            // Get borough by trimming the first part
            String borough = parts[0].trim();
            String boroughName = mapBorough(borough);

            // Further split the second part to separate coordinates and zipcode
            String[] secondPart = parts[1].trim().split("\\s+");
            if (secondPart.length >= 3) { // Check to ensure there are enough parts for coordinates and zipcode
                String longitude = secondPart[0].replaceAll("[()]", ""); // Remove parentheses
                String latitude = secondPart[1].replaceAll("[()]", "");
                String zipcode = secondPart[2].trim(); // fetching zipcode

                // Set output text: borough and zipcode
                output.set(boroughName + "," + zipcode);
                context.write(NullWritable.get(), output);
            }
        } else {
            System.err.println("Invalid input line: " + value.toString());
        }
    }

    // standardizing borough names for consistency in project
    private String mapBorough(String number) {
        switch (number) {
            case "Manhattan":
                return "MANHATTAN";
            case "Bronx":
                return "BRONX";
            case "Brooklyn":
                return "BROOKLYN";
            case "Queens":
                return "QUEENS";
            case "Staten Island":
                return "RICHMOND / STATEN ISLAND";
            default:
                return "";
        }
    }
}