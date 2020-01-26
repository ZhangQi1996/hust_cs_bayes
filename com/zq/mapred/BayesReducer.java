package com.zq.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 文件名，文件中出现的所有单词，文件名，预测的类别
public class BayesReducer extends Reducer<Text, Text, Text, VIntWritable> {

    // 在训练集合中class0的单词数量
    private long class0WordsNumInTrainSet;

    // 在训练集合中class1的单词数量
    private long class1WordsNumInTrainSet;

    // class0的词频映射
    private Map<String, Long> class0WordTimesMap;

    // class1的词频映射
    private Map<String, Long> class1WordTimesMap;

    // 输入预测文档的真实类别（用于测试模型的准确度）
    private Map<String, Integer> docsTrueClassMap;

    // 类别1的对率log[p(c=1)]
    private double logPClass1;

    // 类别0的对率log[p(c=1)]
    private double logPClass0;

    /**
     * 从单路径中读取文件内容
     * @param filePath 单文件路径
     * @param conf
     * @return 返回文件内容
     * @throws IOException
     * 用于读取class0与class1分别词频的文件
     */
    private String getStrContentFromWordTimesFilePath(String filePath, Configuration conf) throws IOException {
        if (filePath == null)
            throw new IOException("请在配置文件中配置word-times.classX.file.path选项");
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);
        InputStream in = null;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            in = fs.open(path);
            IOUtils.copyBytes(in, out, 1024, false);
            return new String(out.toByteArray());
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }

    /**
     * 从多路径中读取文件内容
     * @param filePaths 多文件用逗号隔开
     * @param conf
     * @return
     * @throws IOException
     * 用于读取class0与class1分别词频的文件
     */
    private String[] getStrContentFromWordTimesFilePaths(String filePaths, Configuration conf) throws IOException {
        if (filePaths == null)
            throw new IOException("请在配置文件中配置word-times.classX.file.path选项");
        String[] temp = filePaths.split(",");
        String[] ret = new String[temp.length];
        for (int i = 0; i < temp.length; i++) {
            // 调用单路径
            ret[i] = getStrContentFromWordTimesFilePath(temp[i], conf);
        }
        return ret;
    }

    /**
     * 更新class0的词频映射map
     * @param class0AllContent class class0词频文件的全部内容
     * @throws IOException
     */
    private void updateClass0WordTimesMap(String[] class0AllContent) throws IOException {
        if (class0WordTimesMap == null)
            class0WordTimesMap = new HashMap<String, Long>();
        String temp;
        for (String t: class0AllContent) {
            BufferedReader bufferedReader = new BufferedReader(new StringReader(t));
            while ((temp = bufferedReader.readLine()) != null) {
                String[] wt = temp.split("\\s+");
                // wt[0]是单词 wt[1]是其词频
                class0WordTimesMap.put(wt[0], class0WordTimesMap.getOrDefault(wt[0], 0L) + Long.parseLong(wt[1]));
            }
        }
    }

    /**
     * 参见updateClass0WordTimesMap方法
     * @param class1AllContent
     * @throws IOException
     */
    private void updateClass1WordTimesMap(String[] class1AllContent) throws IOException {
        if (class1WordTimesMap == null)
            class1WordTimesMap = new HashMap<String, Long>();
        String temp;
        for (String t: class1AllContent) {
            BufferedReader bufferedReader = new BufferedReader(new StringReader(t));
            while ((temp = bufferedReader.readLine()) != null) {
                String[] wt = temp.split("\\s+");
                class1WordTimesMap.put(wt[0], class1WordTimesMap.getOrDefault(wt[0], 0L) + Long.parseLong(wt[1]));
            }
        }
    }

    // 用于在hdfs中读取之前完成的词频文件
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        // 读取训练集中的class0的单词数量
        String temp = conf.get("train-set.class0.words-nums");
        if (temp == null)
            throw new IOException("请在配置文件中配置train-set.class0.words-nums选项");
        class0WordsNumInTrainSet = Long.parseLong(temp);
        // 读取训练集中的class1的单词数量
        temp = conf.get("train-set.class1.words-nums");
        if (temp == null)
            throw new IOException("请在配置文件中配置train-set.class1.words-nums选项");
        class1WordsNumInTrainSet = Long.parseLong(temp);
        // 读取类别0，1各自词频文件的存储路径
        String[] class0AllContent = getStrContentFromWordTimesFilePaths(conf.get("file.path.class0.word-times"), conf);
        String[] class1AllContent = getStrContentFromWordTimesFilePaths(conf.get("file.path.class1.word-times"), conf);

        // 更新词频映射map
        updateClass0WordTimesMap(class0AllContent);
        updateClass1WordTimesMap(class1AllContent);

        // 分别计算log[p(c=0)]与log[p(c=1)]
        logPClass0 = Math.log1p(class0WordsNumInTrainSet * 1D / (class0WordsNumInTrainSet + class1WordsNumInTrainSet));
        logPClass1 = Math.log1p(class1WordsNumInTrainSet * 1D / (class0WordsNumInTrainSet + class1WordsNumInTrainSet));

        // 读取测试文档的真实类别文件（该文件存储着所有预测文档的真实类别，用于最后计算准确率）
        String docsTrueClass = conf.get("file.path.docs-true-class");

        // 生成文件名到真实类别的map
        if (docsTrueClass != null) {
            docsTrueClassMap = new HashMap<String, Integer>();
            String contents = getStrContentFromWordTimesFilePath(docsTrueClass, conf);
            BufferedReader bufferedReader = new BufferedReader(new StringReader(contents));
            while ((temp = bufferedReader.readLine()) != null) {
                String[] wc = temp.split("\\s+");
                docsTrueClassMap.put(wc[0], Integer.parseInt(wc[1]));
            }
        }
    }

    /**
     * 计算类别0中单词为某单词的概率为
     * @param word
     * @param wordsNumInDoc
     * @return
     */
    private double computeWordProbabilityInClass0(String word, long wordsNumInDoc) {
        return (1D + class0WordTimesMap.getOrDefault(word, 0L)) / (wordsNumInDoc + class0WordsNumInTrainSet);
    }

    /**
     * 计算类别1中单词为某单词的概率为
     * @param word
     * @param wordsNumInDoc
     * @return
     */
    private double computeWordProbabilityInClass1(String word, long wordsNumInDoc) {
        return (1D + class1WordTimesMap.getOrDefault(word, 0L)) / (wordsNumInDoc + class1WordsNumInTrainSet);
    }

    /**
     * 计算文档类别为0的概率
     * @param doc
     * @return
     */
    private double computeDocProbabilityInClass0(List<String> doc) {
        double ret = 0D;
        long wordsNum = doc.size();
        for (String word: doc) {
            ret += Math.log1p(computeWordProbabilityInClass0(word, wordsNum));
        }
        ret += logPClass0;
        return ret;
    }

    /**
     * 计算文档为1的概率
     * @param doc
     * @return
     */
    private double computeDocProbabilityInClass1(List<String> doc) {
        double ret = 0D;
        long wordsNum = doc.size();
        for (String word: doc) {
            ret += Math.log1p(computeWordProbabilityInClass1(word, wordsNum));
        }
        ret += logPClass1;
        return ret;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 该文档的所有单词
        List<String> list = new ArrayList<String>();
        for (Text t: values) {
            list.add(t.toString());
        }
        // 类别0的概率
        double class0 = computeDocProbabilityInClass0(list);
        // 类别1的概率
        double class1 = computeDocProbabilityInClass1(list);
        int predClass = class0 > class1 ? 0 : 1;
        if (docsTrueClassMap != null) {
            // 获取该文档的真实类别
            int trueClass = docsTrueClassMap.get(key.toString());
            // 用于外部shell脚本计算准确率召回率等评价指标
            System.out.printf("%s\ttrue_class:%d\tpred_class:%d\t%d\n", key.toString(), trueClass, predClass, trueClass == predClass ? 1 : 0);
        }
        // 将文档名与预测结果写出
        context.write(key,  new VIntWritable(predClass));
    }
}
