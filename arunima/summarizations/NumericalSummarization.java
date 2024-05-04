import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.MapWritable;

public class NumericalSummarization {

        public static void main(String[] args) throws Exception {
                if (args.length != 3) {
                        System.err.println("Usage: Numerical Analysis <input path> <output path> <index>");
                        System.exit(-1);
                }
                Job job = Job.getInstance();
                job.setJarByClass(NumericalSummarization.class);
                job.setJobName("data profiling job - Numerical Summarization on clean data");

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                job.setMapperClass(NumericalSummarizationMapper.class);
                job.setReducerClass(NumericalSummarizationReducer.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(MapWritable.class);
                job.setNumReduceTasks(1);

                int index = Integer.parseInt(args[2]);
        		job.getConfiguration().setInt("table_column_index", index);
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}