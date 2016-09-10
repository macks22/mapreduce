package com.macksweeney;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class PatentCiteCountHistogram extends Configured implements Tool {

    private static class PatentCiteCountHistogramMapper extends Mapper<Text, Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable citationCount = new IntWritable();

        /** Output citation count and 1; these will be summed in the reducer. */
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            citationCount.set(Integer.parseInt(value.toString()));
            context.write(citationCount, one);
        }
    }

    private static class PatentCiteCountHistogramReducer extends
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable countOfCounts = new IntWritable();

        /** Sum how many times each count showed up to get the histogram. */
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException
        {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            countOfCounts.set(count);
            context.write(key, countOfCounts);
        }
    }

    /** Default key, value separator is the tab, which is appropriate for the citation count data. */
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, PatentCiteCountHistogram.class.getSimpleName());
        job.setJarByClass(PatentCiteCountHistogram.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(PatentCiteCountHistogramMapper.class);
        job.setReducerClass(PatentCiteCountHistogramReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new PatentCiteCountHistogram(), args);
        System.exit(exitCode);
    }
}
