package com.zq.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

// bayes分类预测的主类
public class Bayes extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Bayes(), args));
    }

    // 删除seq文件
    private void cleanup(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String tempSeqFilePath = conf.get("tmp.seqfile.path");
        if (tempSeqFilePath == null)
            throw new IOException("请设置零食SeqFile的存储路径");
        Path seqFilePath = new Path(tempSeqFilePath);
        FileSystem fs = seqFilePath.getFileSystem(conf);
        fs.delete(seqFilePath, true);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: <generic opts> input output");
            System.exit(-1);
        }
        // 这里要用getConf
        Job job = Job.getInstance(getConf(), "bayes classifier");
        job.setJarByClass(Bayes.class);
        job.setMapperClass(BayesMapper.class);
        job.setReducerClass(BayesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VIntWritable.class);
        // 虽然设置的输入路径为多文档路径，在MySeqFileInputFormat完成输入路径替换为seqfile文件路径
        job.setInputFormatClass(MySeqFileInputFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int exitCode = job.waitForCompletion(true) ? 0: 1;
        // 清除临时seqfile
        cleanup(job);
        return exitCode;
    }
}
