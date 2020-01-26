package com.zq.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class MyFileInputFormat extends FileInputFormat {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // 文件不分片
        return false;
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        WholeFileRecordReader reader = new WholeFileRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }

    static class WholeFileRecordReader extends RecordReader<Text, Text> {

        private FileSplit fileSplit;
        private boolean isOver;
        private Configuration conf;
        private Text val;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            fileSplit = (FileSplit) inputSplit;
            isOver = false;
            conf = taskAttemptContext.getConfiguration();
        }

        @Override
        // 第一是判断是否含有下一个，第二是指向下一个
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!isOver) { // 未处理完
                Path path = fileSplit.getPath();
                FileSystem fs = path.getFileSystem(conf);
                InputStream in = null;
                try {
                    in = fs.open(path);
                    byte[] bytes = new byte[(int) fileSplit.getLength()];
                    IOUtils.readFully(in, bytes, bytes.length, 0);
                    val = new Text(new String(bytes));
                } finally {
                    IOUtils.closeStream(in);
                }
                isOver = true;
                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            // 文件名就是KEY
            return new Text(fileSplit.getPath().getName());
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            // 文件名就是KEY
            return val;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return isOver ? 1F : 0F;
        }

        @Override
        public void close() throws IOException {

        }
    }
}