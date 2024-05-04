import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;

public class WeatherFilter {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");

    public static class WeatherMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");

            if (fields.length == 10 && isValidTimeFormat(fields[0]) && !hasNaN(fields)) {
                // Emit only the value
                context.write(NullWritable.get(), value);
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

        private boolean hasNaN(String[] fields) {
            for (String field : fields) {
                if (field.equalsIgnoreCase("NaN")) {
                    return true;
                }
            }
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WeatherFilter <inputPath> <outputPath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Data Filter");

        job.setJarByClass(WeatherFilter.class);
        job.setMapperClass(WeatherMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
