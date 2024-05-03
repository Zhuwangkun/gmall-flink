package com.htsc.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class SplitWordUtil {

    public static List<String> splitKeyWord(String keyWord) {

        ArrayList<String> words = new ArrayList<>();

        StringReader reader = new StringReader(keyWord);

        //构建分词对象
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        try {

            //获取下一个分词
            Lexeme next = ikSegmenter.next();

            while (next != null) {

                String word = next.getLexemeText();
                words.add(word);

                next = ikSegmenter.next();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return words;
    }

    public static void main(String[] args) {

        System.out.println(splitKeyWord("尚硅谷大数据项目之Flink实时数仓项目"));

    }

}
