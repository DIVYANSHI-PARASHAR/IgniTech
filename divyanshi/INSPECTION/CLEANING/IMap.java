import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

// This mapper outputs the formatted date, status, zipcode, borough name and extracted year

public class IMap extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static final DateTimeFormatter INPUT_DATE_FORMAT = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    private static final DateTimeFormatter OUTPUT_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (!line.startsWith("alpha")) {
            // Split the line by comma
            String[] fields = line.split(",");

            // Check if all columns are present and additional conditions
            if (fields.length >= 20) {
                String lastInspectionDate = fields[5].trim();
                String lastInspStat = fields[6].trim();
                String postcode = fields[14].trim();
                String borough = fields[15].trim();

                // Filter out rows with empty important fields and specific conditions
                if (!postcode.isEmpty() && !borough.isEmpty() && lastInspectionDate.length() == 10 &&
                        (lastInspStat.startsWith("APP") || lastInspStat.startsWith("NOT")) &&
                        postcode.length() == 5 && borough.length() == 2) {
                    String boroughName = mapBorough(borough);

                    // Parse and format the date
                    LocalDate date = LocalDate.parse(lastInspectionDate, INPUT_DATE_FORMAT);
                    String formattedDate = date.atStartOfDay().format(OUTPUT_DATE_FORMAT); // Default time as 00:00

                    // Extract the year from lastInspectionDate
                    String year = String.valueOf(date.getYear());

                    // Construct the output line with selected fields and include the year
                    String output = formattedDate + "," + lastInspStat + "," + postcode + "," + boroughName + ","
                            + year;
                    context.write(NullWritable.get(), new Text(output));
                }
            }
        }
    }
    // convertin borough codes to names
    private String mapBorough(String number) {
        switch (number) {
            case "MN":
                return "MANHATTAN";
            case "BX":
                return "BRONX";
            case "BK":
                return "BROOKLYN";
            case "QN":
                return "QUEENS";
            case "SI":
                return "RICHMOND / STATEN ISLAND";
            default:
                return "";
        }
    }
}