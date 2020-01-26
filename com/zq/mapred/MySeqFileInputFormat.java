package com.zq.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

// <Text, Text> 文件名，内容
public class MySeqFileInputFormat extends SequenceFileInputFormat<Text, Text> {

    // 对listStatus函数进行重写，对原方法内容不做改动，只是在之前加入了将多个文件整合成单个seqfile
    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        // 以下为加入
        Configuration conf = job.getConfiguration();
        // 获得所有的输入文件
        List<FileStatus> files = super.listStatus(job);
        Iterator i$ = files.iterator();
        FileSystem fs = null;
        InputStream in = null;
        // seqfile配置项
        // 读取自定义的外部配置项，目的是为了解耦合
        String tempSeqPath = conf.get("tmp.seqfile.path");
        if (tempSeqPath == null)
            throw new IOException("请设置临时SeqFile的存储路径");
        Path seqFilePath = new Path(tempSeqPath);
        // 创建
        SequenceFile.Writer writer = SequenceFile.createWriter(seqFilePath.getFileSystem(conf), conf, seqFilePath, Text.class, Text.class);
        try {
            // 将所有文件写入一个seqfile中
            while (i$.hasNext()) {
                FileStatus file = (FileStatus) i$.next();
                Path path = file.getPath();
                long length = file.getLen();
                if (length != 0L) {
                    fs = path.getFileSystem(conf);
                    in = fs.open(path);
                    byte[] bs = new byte[(int) length];
                    IOUtils.readFully(in, bs, 0, (int) length);
                    Text fileContent = new Text(new String(bs));
                    Text fileName = new Text(path.getName());
                    writer.append(fileName, fileContent);
                    IOUtils.closeStream(in);
                }

            }
        } finally {
            IOUtils.closeStream(writer);
            IOUtils.closeStream(in);
        }
        // 将原来的多文件输入路径改写为临时文件为输入路径
        // 即执行map之前是从临时文件读取数据而不是在原来路径中读取数据
        setInputPaths((Job) job, seqFilePath);
        // 以上为加入
        // 调用原方法
        return super.listStatus(job);
    }
}
