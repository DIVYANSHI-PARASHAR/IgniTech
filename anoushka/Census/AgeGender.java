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

public class AgeGender 
{
    public static class AgeGenderMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            if(fields.length >= 218)
            {
                if(fields[0].equals("GEO_ID") || fields[0].equals("Geography")) // ignore header lines
                    return;
                String year = context.getConfiguration().get("YEAR");
                String zipcode = fields[1].split(" ")[1];

                StringBuilder usefulColumnsBuilder = new StringBuilder();

                int tot=Integer.parseInt(fields[2]);
                int male=Integer.parseInt(fields[4]);
                int female=Integer.parseInt(fields[6]);

                usefulColumnsBuilder.append(fields[2]);
                usefulColumnsBuilder.append("\t");
                usefulColumnsBuilder.append(fields[4]);
                usefulColumnsBuilder.append("\t");
                usefulColumnsBuilder.append(fields[6]);
                usefulColumnsBuilder.append("\t");
                if(Integer.parseInt(year)<=2016)
                {
                    for(int i=8;i<=110;i+=6)
                    {
                        float tot_per=0;
                        float male_perc=0;
                        float female_perc=0;
                        if(!(fields[i].equals("**") || fields[i].equals("-")))
                            tot_per=Float.parseFloat(fields[i]);
                        if(!(fields[i+2].equals("**") || fields[i+2].equals("-")))
                            male_perc=Float.parseFloat(fields[i+2]);
                        if(!(fields[i+4].equals("**") || fields[i+4].equals("-")))
                            female_perc=Float.parseFloat(fields[i+4]);

                        float tot_val=tot_per*tot;
                        float male_val=male_perc*male;
                        float female_val=female_perc*female;
                        usefulColumnsBuilder.append((int)tot_val);
                        usefulColumnsBuilder.append("\t");
                        usefulColumnsBuilder.append((int)male_val);
                        usefulColumnsBuilder.append("\t");
                        usefulColumnsBuilder.append((int)female_val);
                        if(i<110)
                            usefulColumnsBuilder.append("\t");

                    }
                }
                else
                {
                    for (int i = 2; i <= 114; i += 2) 
                    {
                        if(fields[i].equals("**") || fields[i].equals("-"))
                            usefulColumnsBuilder.append("0");
                        else
                            usefulColumnsBuilder.append(fields[i]);
                        if (i < 114) {
                            usefulColumnsBuilder.append("\t");
                        }
                    }
                }
                String usefulColumns = usefulColumnsBuilder.toString();
                context.write(new Text(year + "\t" + zipcode), new Text(usefulColumns));
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
            job = Job.getInstance(conf, "AgeGender cleaning - " + year);
            job.setJarByClass(AgeGender.class);
            job.setMapperClass(AgeGenderMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0] + "/AgeGender" + year +".txt")); // Input path for current year
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + year)); // Output path for current year
            job.waitForCompletion(true);
        }
    }
}

