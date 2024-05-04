import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// This driver will be used with multiple mappers
// For profiling of raw/cleaned data to calculate total inspection per borough, use it with boroProfileIMap.java
// For calculating total inspection per borough year wise, use it with YearBoroughImap.java

public class boroProfileIDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RProfileIDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "R Profile Inspection Count");

        job.setJarByClass(boroProfileIDriver.class);
        job.setMapperClass(boroProfileIMap.class); // YearBoroughIMap for yearwise results
        job.setReducerClass(boroProfileIReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(5);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}