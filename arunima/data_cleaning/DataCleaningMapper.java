import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;

public class DataCleaningMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private final int[] columnsToRead = {5, 6, 4, 16, 15, 1, 21, 24, 25, 3, 7, 12, 26, 27};

/*
    1 - incident_datetime
    3 - alarm_box number
    4 - alarm_box_location
    5 - incident_borough
    6 - Zipcode
    7 - police precint
    12 - alarm source
    15 - incident_classification
    16 - incident_calssification_group
    21 - incident_close_time
    24 - incident_respond_seconds
    25 - travel_tm_seconds
    26 - engines
    27 - ladders
*/
    private Text mapText = new Text();

    private static HashMap<String, String> roadAbbreviations = new HashMap<>();
 
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] columnValues = value.toString().split(",");
        
        // Checking if columnValues conforms to the schema
        if (columnValues.length != 29) {
                        return;
        }

        // Removing header column
        if(columnValues[0].equals("STARFIRE_INCIDENT_ID")) return;

        // Zipcode and Borough are mandatory fields
        if(columnValues[5].length() == 0 || columnValues[6].length()==0) return;

        // Filtering out non-fire incidents from the dataset
        if(!columnValues[16].toLowerCase().contains("fires")) return;

        List<String> columns = new ArrayList<>();

        
        
        for (int columnIndex : columnsToRead) {
            // Remove inconsistent/missing field data
            if(columnValues[columnIndex]==null || columnValues[columnIndex].toLowerCase().equals("null") || columnValues[columnIndex].length()==0) return;
            columns.add(columnValues[columnIndex]);
        }

        // Add time column for weather dataset
        String recordedTime = columnValues[1];
        columns.add(addTimeColumnForWeather(recordedTime));

	// Update incident date time and incident close time in timestamp format
        String incident_datetime = columns.get(5);
        columns.remove(5);
        columns.add(5, modifyTimeColumn(incident_datetime));

        String incident_close_time = columns.get(6);
        columns.remove(6);
        columns.add(6, modifyTimeColumn(incident_close_time));

        // Make location data consistent
        String locationColumn = columns.get(2);
        columns.remove(2);
        columns.add(2, modifyLocationColumn(locationColumn));

        mapText.set(String.join(",", columns));
        context.write(NullWritable.get(), mapText);
        }

    private String addTimeColumnForWeather(String inputTime) {
        SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");

        try {
            Date date = inputFormat.parse(inputTime);

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            if (calendar.get(Calendar.MINUTE) >= 30) {
                // If so, round up to the next hour
                calendar.add(Calendar.HOUR_OF_DAY, 1);
            }

            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);

            Date roundedTime = calendar.getTime();

            return outputFormat.format(roundedTime);
        } catch (ParseException e) {
            e.printStackTrace();
            return "9999-09-09 09:09";
        }
    }

    private String modifyTimeColumn(String inputTime) {
        SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            Date date = inputFormat.parse(inputTime);
            return outputFormat.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
            return "9999-9-9 9:9:9";
        }
    }

    private static String modifyLocationColumn(String location) {
        if (location == null || location.length()==0) {
          return location;
        }

        location=" "+location+" ";

        roadAbbreviations.put(" STREET ", " ST ");
        roadAbbreviations.put(" AVENUE ", " AVE ");
        roadAbbreviations.put(" AV ", " AVE ");
        roadAbbreviations.put(" AV. ", " AVE ");
        roadAbbreviations.put(" ROAD ", " RD ");
        roadAbbreviations.put(" RD. ", " RD ");
        roadAbbreviations.put(" LANE ", " LN ");
        roadAbbreviations.put(" DRIVE ", " DR ");
        roadAbbreviations.put(" BOULEVARD ", " BLVD ");
        roadAbbreviations.put(" PLACE ", " PL ");
        roadAbbreviations.put(" SQUARE ", " SQ ");
        roadAbbreviations.put(" SQ. ", " SQ ");
        roadAbbreviations.put(" TURNPIKE ", " TPKE ");
        roadAbbreviations.put(" HIGHWAY ", " HWY ");
        roadAbbreviations.put(" PARKWAY ", " PKWY ");
        roadAbbreviations.put(" EXPRESSWAY ", " EXPY ");
        roadAbbreviations.put(" PLAZA ", " PZ ");
        roadAbbreviations.put(" COURT" , " CT ");
        roadAbbreviations.put(" CT. ", " CT ");

        String result = location.toUpperCase();
        for(Map.Entry<String,String> entry: roadAbbreviations.entrySet()){
            result = result.replaceAll(entry.getKey(), entry.getValue());
        }
    
        return result;
      }
}
