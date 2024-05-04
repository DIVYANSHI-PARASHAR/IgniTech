import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class MeanIncome 
{
    public static class MeanIncomeMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            if(fields.length >= 110)
            {
                if(fields[0].equals("GEO_ID") || fields[0].equals("Geography")) // ignore header lines
                    return;
                String year = context.getConfiguration().get("YEAR");
                if(Integer.parseInt(year)<=2016)
                {
                    String zipcode = fields[1].split(" ")[1];
                    String usefulColumns = fields[4] + "\t" + fields[64] + "\t" + fields[68] + "\t" + fields[76]; 
                    String[] usefulFields = usefulColumns.split("\t");
                    for (int i = 0; i < usefulFields.length; i++) 
                    {
                        if (usefulFields[i].equals("N") || usefulFields[i].equals("-"))
                         {
                            usefulFields[i] = "0";
                        }
                    }
                    String cleaned_usefulColumns = String.join("\t", usefulFields);
                    context.write(new Text(year + "\t" + zipcode), new Text(cleaned_usefulColumns));
                }
                else
                {
                    String zipcode = fields[1].split(" ")[1];
                    String usefulColumns = fields[114] + "\t" + fields[152] + "\t" + fields[154] + "\t" + fields[158]; 
                    String[] usefulFields = usefulColumns.split("\t");
                    for (int i = 0; i < usefulFields.length; i++) 
                    {
                        if (usefulFields[i].equals("N") || usefulFields[i].equals("-"))
                         {
                            usefulFields[i] = "0";
                        }
                    }
                    String cleaned_usefulColumns = String.join("\t", usefulFields);
                    context.write(new Text(year + "\t" + zipcode), new Text(cleaned_usefulColumns));
                }
            }
        }
    }
   
     public static void main(String[] args) throws Exception 
     {
        Configuration conf;
        Job job;
        String[] years = {"2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020"};
        
        for (String year : years) 
        {
            conf = new Configuration();
            conf.set("YEAR", year);
            job = Job.getInstance(conf, "Mean Income cleaning - " + year);
            job.setJarByClass(MeanIncome.class);
            job.setMapperClass(MeanIncomeMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0] + "/MeanIncome" + year +".txt")); // Input path for current year
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + year)); // Output path for current year
            job.waitForCompletion(true);
        }
    }
}

