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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class DataCombiner
{

    // Mapper for MeanIncome Data
    public static class MeanIncomeMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] fields = value.toString().split("\t");
            if (fields.length >= 6) 
            {
                String year= fields[0];
                String zipcode = fields[1];
                StringBuilder valBuilder = new StringBuilder();
                for (int i = 2; i <= 5; i++) 
                {
                    valBuilder.append(fields[i]);
                    if (i < 5) {
                        valBuilder.append("\t");
                    }
                }
                String val = valBuilder.toString();
                context.write(new Text(year + "\t" + zipcode), new Text("MeanIncome:" + val));
            }
        }
    }

    // Mapper for MedianIncome Data
    public static class MedianIncomeMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] fields = value.toString().split("\t");
            if (fields.length >= 6) 
            {
                String year= fields[0];
                String zipcode = fields[1];
                StringBuilder valBuilder = new StringBuilder();
                for (int i = 2; i <= 5; i++) 
                {
                    valBuilder.append(fields[i]);
                    if (i < 5) {
                        valBuilder.append("\t");
                    }
                }
                String val = valBuilder.toString();
                context.write(new Text(year + "\t" + zipcode), new Text("MedianIncome:" + val));
            }
        }
    }

    // Mapper for AgeGender Data
    public static class AgeGenderMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] fields = value.toString().split("\t");
            if (fields.length >= 5) 
            {
                String year= fields[0];
                String zipcode = fields[1];
                StringBuilder valBuilder = new StringBuilder();
                for (int i = 2; i <= 4; i++) 
                {
                    valBuilder.append(fields[i]);
                    if (i < 4) {
                        valBuilder.append("\t");
                    }
                }
                String val = valBuilder.toString();
                context.write(new Text(year + "\t" + zipcode), new Text("AgeGender:" + val));
            }
        }
    }

    // Reducer
    public static class CombineReducer extends Reducer<Text, Text, Text, Text> 
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            String mean = null;
            String median = null;
            String age=null;
            String zipcode=key.toString().split("\t")[1];
            int zc=Integer.parseInt(zipcode);
            String bor="";
            if(zc>=10001 && zc<=10282)
                bor="MANHATTAN";
            else if(zc>=10301 && zc<=10314)
                bor="RICHMOND / STATEN ISLAND";
            else if(zc>=10451 && zc<=10475)
                bor="BRONX";
            else if(zc>=11201 && zc<=11256)
                bor="BROOKLYN";
            else
                bor="QUEENS";
            for (Text value : values) 
            {
                String[] parts = value.toString().split(":");
                if (parts.length == 2) 
                {
                    String dataType = parts[0];
                    String dataValue = parts[1];
                    if (dataType.equals("MeanIncome")) 
                        mean = dataValue;
                    else if (dataType.equals("MedianIncome"))
                        median= dataValue;
                    else if (dataType.equals("AgeGender"))
                        age= dataValue;
                }
            }
            String null5="0"+"\t"+"0"+"\t"+"0"+"\t"+"0"+"\t"+"0";
            String null3="0"+"\t"+"0"+"\t"+"0"+"\t";
            String combinedData = bor + "\t" + (mean != null ? mean : null5) + "\t" +
                              (median != null ? median : null5) + "\t" +
                              (age != null ? age : null3);
            context.write(key, new Text(combinedData));

        }
    }

    public static void main(String[] args) throws Exception 
    {
        if (args.length != 4) 
        {
            System.err.println("Usage: DataCombiner <meanInput> <medianInput> <AgeInput> <outputPath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Mean,median income and Age Data Combine");
        job.setJarByClass(DataCombiner.class);

        job.setReducerClass(CombineReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MeanIncomeMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MedianIncomeMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, AgeGenderMapper.class);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
