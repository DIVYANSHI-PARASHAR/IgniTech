import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MonthTemp
{
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
    public static class MonthTempMapper extends Mapper<LongWritable, Text, Text, FloatWritable> 
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString();
            String[] fields = line.split("\t");

            // Only consider valid rows
            if (fields.length == 10 && isValidTimeFormat(fields[0]) && !hasNaN(fields)) 
            {
                String month = getMonthName(fields[0]);
                float temp= Float.parseFloat(fields[1]);
                context.write(new Text(month), new FloatWritable(temp));
            }
        }

        private boolean isValidTimeFormat(String time) {
            try {
                Date parsedDate = DATE_FORMAT.parse(time);
                return time.equals(DATE_FORMAT.format(parsedDate));
            } catch (ParseException e) {
                return false;
            }
        }

        private boolean hasNaN(String[] fields) 
        {
            for (String field : fields) {
                if (field.equalsIgnoreCase("NaN")) {
                    return true;
                }
            }
            return false;
        }

        private String getMonthName(String date) 
        {
            try 
            {
                Date parsedDate = DATE_FORMAT.parse(date);
                java.util.Calendar cal = java.util.Calendar.getInstance();
                cal.setTime(parsedDate);
                int month = cal.get(java.util.Calendar.MONTH);
                String[] monthNames = new String[] { "January", "February", "March", "April", "May", "June", "July",
                        "August", "September", "October", "November", "December" };
                return monthNames[month];
            } 
            catch (ParseException e) {
                return "Invalid Date";
            }
        }
    }
    
    // Reducer to calculate min max 
    public static class MonthTempReducer extends Reducer<Text, FloatWritable, Text, Text> 
    {
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException 
        {
            float minTemp = Float.MAX_VALUE;
            float maxTemp = Float.MIN_VALUE;
            for (FloatWritable value : values) 
            {
                minTemp=Math.min(minTemp,value.get());
                maxTemp=Math.max(maxTemp,value.get());
            }
            String answer="Min: "+ minTemp + " Max: " + maxTemp;
            context.write(key, new Text(answer));
        }
    }


    public static void main(String[] args) throws Exception 
    {
        if (args.length != 2) {
            System.err.println("Usage: MonthTemp <inputPath> <outputPath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Monthly Temperature Profiler");

        job.setJarByClass(MonthTemp.class);
        job.setMapperClass(MonthTempMapper.class);
        job.setReducerClass(MonthTempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
