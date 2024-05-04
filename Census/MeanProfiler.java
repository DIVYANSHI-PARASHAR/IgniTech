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

public class MeanProfiler
{
    public static class MeanProfilerMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString();
            String[] fields = line.split("\t");

            String year= fields[0];
            String zc=fields[1];
            String mi=fields[2];
            String ans= zc +"," + mi;
            context.write(new Text(year), new Text(ans));
        }
    }
    
    // Reducer to calculate min max 
    public static class MeanProfilerReducer extends Reducer<Text, Text, Text, Text> 
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            float minIncome = Integer.MAX_VALUE;
            float maxIncome = Integer.MIN_VALUE;
            String minZC=null;
            String maxZC=null;
            for (Text value : values) 
            {
                String []parts=value.toString().split(",");
                if(parts.length>=2)
                {
                    String zc=parts[0];
                    int mi_val=Integer.parseInt(parts[1]);
                    if(mi_val<minIncome)
                    {
                        minIncome=mi_val;
                        minZC=zc;
                    }
                    if(mi_val>maxIncome)
                    {
                        maxIncome=mi_val;
                        maxZC=zc;
                    }
                }
            }
            String answer="Minimum: ZC= "+minZC+ " Value= " + minIncome + " Maximum: ZC= "+maxZC+ " Value= " + maxIncome;
            context.write(key, new Text(answer));
        }
    }


    public static void main(String[] args) throws Exception 
    {
        if (args.length != 2) {
            System.err.println("Usage: MeanProfiler <inputPath> <outputPath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Mean Profiler");

        job.setJarByClass(MeanProfiler.class);
        job.setMapperClass(MeanProfilerMapper.class);
        job.setReducerClass(MeanProfilerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
