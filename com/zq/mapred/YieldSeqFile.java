package com.zq.mapred;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class YieldSeqFile extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        FileInputFormat format = new FileInputFormat() {
            @Override
            public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                return null;
            }
        };
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new YieldSeqFile(), args));
    }
}
