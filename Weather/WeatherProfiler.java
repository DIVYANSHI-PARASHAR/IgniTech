import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherProfiler 
{
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
    public static class WeatherProfilerMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString();
            String[] fields = line.split("\t");
            // If any field has NaN then I'm recording the date and count
            if (fields.length == 10 && isValidTimeFormat(fields[0]) && hasNaN(fields)) 
            {
                String date=fields[0].substring(0,10); // extract yyyy-MM-dd part
                context.write(new Text(date), new IntWritable(1));
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

    // Reducer just outputs the date and number of hours where there are null values
    public static class WeatherProfilerReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            int sum=0;
            for (IntWritable value : values) 
                sum += value.get();
            context.write(key, new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception 
    {
        if (args.length != 2) {
            System.err.println("Usage: WeatherProfiler <inputPath> <outputPath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Profiler Filter");

        job.setJarByClass(WeatherProfiler.class);
        job.setMapperClass(WeatherProfilerMapper.class);
        job.setReducerClass(WeatherProfilerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
