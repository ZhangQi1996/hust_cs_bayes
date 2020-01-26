package com.zq.mapred;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordParser {
    // 正则
    private static final String REGEX = "^\\s*\\S+\\s*$";

    // 静态工厂模式
    private WordParser() {}

    // 解析判断所给内容是否符合单词形式
    public static boolean match(String targetString) {
        return targetString.matches(REGEX);
    }

    // 仅仅当解析有效的到时候才做单词处理
    public static String getWord(String targetString) {
        if (!match(targetString))
            return null;
        // 去除前后空格
        Matcher matcher = Pattern.compile("\\S+").matcher(targetString);
        if (matcher.find())
            return matcher.group();
        else
            return null;
    }

}
