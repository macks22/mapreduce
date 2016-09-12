package com.macksweeney;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by msweeney on 9/10/16.
 */
public class AverageByCountry extends Configured implements Tool {

    private static class AvgByCountryMapper extends Mapper<Text, Text, Text, Text> {

        /** Output a number_of_claims, 1 for each country key. */
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",", -1);
            String country = fields[4];
            String numClaims = fields[8];
            if (!numClaims.isEmpty() && !numClaims.startsWith("\"")) {
                context.write(new Text(country), new Text(numClaims + ",1"));
            }
        }
    }

    private static class AvgByCountryCombiner extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            double sum = 0;
            int count = 0;
            for (Text value : values) {
                String[] fields = value.toString().split(",");
                sum += Double.parseDouble(fields[0]);
                count += Integer.parseInt(fields[1]);
            }

            // Write the same format as the mapper does -- this is what the Reducer expects.
            context.write(key, new Text(sum + "," + count));
        }
    }

    private static class AvgByCountryReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            double sum = 0;
            int count = 0;
            for (Text value : values) {
                String fields[] = value.toString().split(",");
                sum += Double.parseDouble(fields[0]);
                count += Integer.parseInt(fields[1]);
            }
            context.write(key, new DoubleWritable(sum / count));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        Job job = Job.getInstance(conf, AverageByCountry.class.getSimpleName());
        job.setJarByClass(AverageByCountry.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(AvgByCountryMapper.class);
        job.setCombinerClass(AvgByCountryCombiner.class);
        job.setReducerClass(AvgByCountryReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new AverageByCountry(), args);
        System.exit(exitCode);
    }
}
