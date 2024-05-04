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

public class MedianIncome 
{
    public static class MedianIncomeMapper extends Mapper<LongWritable, Text, Text, Text> 
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
                    String usefulColumns = fields[4] + "\t" + fields[8] + "\t" + fields[12] + "\t" + fields[20]; 
                    String[] usefulFields = usefulColumns.split("\t");
                    for (int i = 0; i < usefulFields.length; i++) 
                    {
                        if (usefulFields[i].equals("**") || usefulFields[i].equals("-") || usefulFields[i].equals("(X)"))
                        {
                            usefulFields[i] = "0";
                        }
                        if(usefulFields[i].equals("\"250,000+\""))
                            usefulFields[i]="250000";
                    }
                    String cleaned_usefulColumns = String.join("\t", usefulFields);
                    context.write(new Text(year + "\t" + zipcode), new Text(cleaned_usefulColumns));
                }
                else
                {
                    String zipcode = fields[1].split(" ")[1];
                    String usefulColumns = fields[162] + "\t" + fields[164] + "\t" + fields[166] + "\t" + fields[170]; 
                    String[] usefulFields = usefulColumns.split("\t");
                    for (int i = 0; i < usefulFields.length; i++) 
                    {
                        if (usefulFields[i].equals("**") || usefulFields[i].equals("-") || usefulFields[i].equals("(X)"))
                        {
                            usefulFields[i] = "0";
                        }
                        if(usefulFields[i].equals("\"250,000+\""))
                            usefulFields[i]="250000";
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
            job = Job.getInstance(conf, "Median Income cleaning - " + year);
            job.setJarByClass(MedianIncome.class);
            job.setMapperClass(MedianIncomeMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0] + "/MedianIncome" + year +".txt")); // Input path for current year
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + year)); // Output path for current year
            job.waitForCompletion(true);
        }
    }
}

