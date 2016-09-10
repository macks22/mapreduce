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

    private static class PatentCitingCounterMapper extends Mapper<Text, Text, Text, Text> {

        /** Reverse citing -> cited relationship to cited -> citing. */
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }
    }

    private static class PatentCitingCounterReducer extends Reducer<Text, Text, Text, IntWritable> {

        /** For each patent, output the number of patents citing it. */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            int count = 0;
            for (Text citing : values) {
                count += 1;
            }
            context.write(key, new IntWritable(count));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        Job job = Job.getInstance(conf, PatentCiteCountHistogram.class.getSimpleName());
        job.setJarByClass(PatentCiteCountHistogram.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(PatentCitingCounterMapper.class);
        job.setReducerClass(PatentCitingCounterReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new PatentCiteCountHistogram(), args);
        System.exit(exitCode);
    }
}
