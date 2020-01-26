package com.zq.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// 统计词频的mapper类
public class CountWordTimesInClassMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 由于所给文档中每行就一个单词，故val就是一个单词
        String word = value.toString();
        // 调用解析类解析单词，判断单词是否合法，解析val，当有效时才map
        if (WordParser.match(word)) { // 解析有效
            // 解析有效时，才返回处理后的单词（比如去除单词前后的空格）
            word = WordParser.getWord(word);
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
