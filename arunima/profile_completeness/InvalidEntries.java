import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvalidEntries {
        public static void main(String[] args) throws Exception {
                if (args.length != 2) {
                        System.err.println("Usage: Invalid <input path> <output path>");
                        System.exit(-1);
                }
                Job job = Job.getInstance();
                job.setJarByClass(InvalidEntries.class);
                job.setJobName("Data profiling - count invalid entries");

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                job.setMapperClass(InvalidEntriesMapper.class);
                job.setReducerClass(InvalidEntriesReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                job.setNumReduceTasks(1);

                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
