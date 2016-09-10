package com.macksweeney;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.util.StringJoiner;

/**
 * Created by msweeney on 9/10/16.
 */
public class PatentAnalyzer extends Configured implements Tool {

    private static class PatentAnalyzerMapper extends Mapper<Text, Text, Text, Text> {

        /** Reverse citing -> cited relationship to cited -> citing. */
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }
    }

    private static class PatentAnalyzerReducer extends Reducer<Text, Text, Text, Text> {

        /** Group patents that cite a particular patent. */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            StringJoiner joiner = new StringJoiner(",");
            for (Text citingId : values) {
                joiner.add(citingId.toString());
            }
            context.write(key, new Text(joiner.toString()));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        Job job = Job.getInstance(conf, PatentAnalyzer.class.getSimpleName());
        job.setJarByClass(PatentAnalyzer.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(PatentAnalyzerMapper.class);
        job.setReducerClass(PatentAnalyzerReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new PatentAnalyzer(), args);
        System.exit(exitCode);
    }
}
