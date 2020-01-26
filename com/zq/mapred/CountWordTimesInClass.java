package com.zq.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// 用于统计词频的主类
public class CountWordTimesInClass extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        if (strings.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(CountWordTimesInClass.class);
        job.setJobName("CountWordTimesInClass");

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        // 设置mapper
        job.setMapperClass(CountWordTimesInClassMapper.class);
        // 设置reducer
        job.setReducerClass(CountWordTimesInClassReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    // 入口
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CountWordTimesInClass(), args);
        System.exit(exitCode);
    }
}
