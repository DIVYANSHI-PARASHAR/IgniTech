import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.net.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

// This mapper is for fetching the zipcodes from eahc pair of longitude and latitude of a hydrant
// using revese geocoding from GOOGLE MAPS PLATFORM'S GEOCODING API
// It's output is the borough name (key) along with the lat-long point and zipcode (value)

public class hmi extends Mapper<LongWritable, Text, Text, Text> {

    private Text borough = new Text();
    private Text latLongZ = new Text();


    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Split the input line by comma
        String[] parts = value.toString().split(",");

        // Extract borough and lat long point
        borough.set(parts[0].trim());
        latLongZ.set(parts[1].trim());

        // Extract latitude and longitude
        String[] coordinates = parts[1].replaceAll("[^0-9.,\\s-]", "").trim().split("\\s+");
        String lng = coordinates[0];
        String lat = coordinates[1];

        // Query Google Geocoding API
        String apiUrl = "https://maps.googleapis.com/maps/api/geocode/json?latlng=" + lat + "," + lng
                + "&key=KEY";
        try {
            HttpURLConnection connection = (HttpURLConnection) new URL(apiUrl).openConnection();
            connection.setRequestMethod("GET");
            BufferedReader input = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            String output = "";
            String responseLine;
            while ((responseLine = input.readLine()) != null) {
                output = output + responseLine;
            }
            input.close();
            output = output.replaceAll("\\s+", "");
            int index = output.indexOf("postal_code");

            // extractinf zipcode using start and end index
            String zipCode = output.substring(index - 17, index - 12);

            latLongZ.set(latLongZ.toString() + " " + zipCode);
            context.write(borough, latLongZ);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
