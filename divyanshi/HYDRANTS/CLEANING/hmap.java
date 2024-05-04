import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

// This mapper just emits the borough name and location (the first 2 columns)
// after removing the rows with empty values

public class hmap extends Mapper<LongWritable, Text, NullWritable, Text> {
    private Text borough = new Text();
    private Text location = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Splitting the CSV row
        String[] columns = value.toString().split(",");

        // taking only first 2 columns (borough name and the_geom comprising the latitude, longitude)
        if (columns.length > 7 && !columns[0].isEmpty() && !columns[1].isEmpty()) {
            String boroughName = mapBorough(columns[1].trim());
            if (!boroughName.isEmpty()) {
                borough.set(boroughName);
                location.set(columns[0].trim()); // latitude,longitude
                context.write(NullWritable.get(), new Text(borough + "," + location));
            }
        }
    }

    // converting borough numbers to borough name
    private String mapBorough(String number) {
        switch (number) {
            case "1":
                return "Manhattan";
            case "2":
                return "Bronx";
            case "3":
                return "Brooklyn";
            case "4":
                return "Queens";
            case "5":
                return "Staten Island";
            default:
                return "";
        }
    }
}