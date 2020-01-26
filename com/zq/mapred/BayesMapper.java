package com.zq.mapred;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

// <Text, Text, Text, Text>文件名，文件内容， 文件名，单词
public class BayesMapper extends Mapper<Text, Text, Text, Text> {
    // 由于是读取seqfile（将多个文档整个城一个seqfile），故一条记录就是一个文档
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // 读取单个文档内容
        String content = value.toString();
        BufferedReader bufferedReader = new BufferedReader(new StringReader(content));
        String word;
        while ((word = bufferedReader.readLine()) != null) {
            // 文件名，每个单词
            context.write(key, new Text(word));
        }
    }
}
