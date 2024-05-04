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

public class DecennialCombiner
{

    // Mapper for Population Data
    public static class PopulationMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] fields = value.toString().split("\t");
            if (fields.length >= 3) 
            {
                if(fields[0].equals("GEO_ID")|| fields[0].equals("Geography"))
                    return;
                String zipcode = fields[1].split(" ")[1];
                String population = fields[2]; // Assuming Population is at index 2
                context.write(new Text(zipcode), new Text("population\t" + population));
            }
        }
    }

    // Mapper for Housing Data
    public static class HousingMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] fields = value.toString().split("\t");
            if (fields.length >= 3) 
            {
                if(fields[0].equals("GEO_ID")|| fields[0].equals("Geography"))
                    return;
                String zipcode = fields[1].split(" ")[1];
                String housingUnits = fields[2]; // Assuming H1_001N is at index 2
                context.write(new Text(zipcode), new Text("housing\t" + housingUnits));
            }
        }
    }

    // Reducer
    public static class CombineReducer extends Reducer<Text, Text, Text, Text> 
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            String population = null;
            String housingUnits = null;
            String zipcode=key.toString();
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
                String[] parts = value.toString().split("\t");
                if (parts.length == 2) 
                {
                    String dataType = parts[0];
                    String dataValue = parts[1];
                    if (dataType.equals("population")) 
                        population = dataValue;
                    else if (dataType.equals("housing"))
                        housingUnits = dataValue;
                }
            }
            if(population != null && housingUnits != null)
                context.write(key, new Text(bor + "\t" + population + "\t" + housingUnits));
            else if(population!=null)
                context.write(key, new Text(bor +"\t" + population + "\t"));
            else if(housingUnits!=null)
                context.write(key, new Text(bor +"\t"+ housingUnits + "\t"));
        }
    }

    public static void main(String[] args) throws Exception 
    {
        if (args.length != 3) 
        {
            System.err.println("Usage: DecennialCombiner <populationInput> <housingInput> <outputPath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Decennial Data Combine");
        job.setJarByClass(DecennialCombiner.class);

        job.setReducerClass(CombineReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Set Multiple Mappers
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PopulationMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, HousingMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
