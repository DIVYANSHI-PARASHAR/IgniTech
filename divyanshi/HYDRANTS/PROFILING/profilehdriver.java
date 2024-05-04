import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class profilehdriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hydrant Profile");

        // Set the jar class
        job.setJarByClass(profilehdriver.class);

        // Set the mapper class
        job.setMapperClass(profilehmap.class);

        // Set the reducer class
        job.setReducerClass(profilehreduce.class);

        // Set the output key class
        job.setOutputKeyClass(Text.class);

        // Set the output value class
        job.setOutputValueClass(IntWritable.class);

        // Set the input file path
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Set the output file path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(5);
        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}