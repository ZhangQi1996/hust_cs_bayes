package com.zq.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// 词频统计reducer类
public class CountWordTimesInClassReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 词频统计reduce
     * @param key 单词
     * @param values 全为1的迭代器
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int length = 0;
        for (IntWritable i: values)
            length++; // 词频
        context.write(key, new IntWritable(length));
    }
}
