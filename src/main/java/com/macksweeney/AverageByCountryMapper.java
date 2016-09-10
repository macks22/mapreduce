package com.macksweeney;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
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
public class AverageByCountryMapper extends Configured implements Tool {

    private static class AvgByAttrMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",", -1);
            String country = fields[4];
            String numClaims = fields[8];
            if (!numClaims.isEmpty() && !numClaims.startsWith("\"")) {
                context.write(new Text(country), new Text(numClaims + ",1"));
            }
        }
    }

    private static class AvgByAttrReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            super.reduce(key, values, context);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        Job job = Job.getInstance(conf, AverageByCountryMapper.class.getSimpleName());
        job.setJarByClass(AverageByCountryMapper.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(AvgByAttrMapper.class);
        job.setReducerClass(AvgByAttrReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new AverageByCountryMapper(), args);
        System.exit(exitCode);
    }
}
