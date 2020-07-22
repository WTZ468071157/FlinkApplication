package com.wtz.flink.wordcount;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;

/**
 * @author tiezhu
 * Date 2020/7/19 周日
 * Company dtstack
 */
public class WordCountSQL {

    public static class WordCount {
        public String word;
        public Integer count;

        public WordCount() {
        }

        public WordCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        String word = "this is a text for flink SQL";
        String[] input = word.split("\\W+");

        ArrayList<WordCount> list = new ArrayList<>();

        for(String item : input) {
            list.add(new WordCount(item, 1));
        }
        DataSet<WordCount> wordInput = env.fromCollection(list);

        // DataSet 转化为 Table, 并指定字段类型
        Table table = tableEnv.fromDataSet(wordInput, "word, count");
        table.printSchema();

        // 注册一个表
        tableEnv.createTemporaryView("WordCount", table);
        // 查询
        Table sqlQuery = tableEnv.sqlQuery("SELECT word AS word, sum(`count`) AS `count` FROM WordCount GROUP BY word");
        // 将表转化为DataSet
        TableSchema schema = sqlQuery.getSchema();
        System.out.println(schema);
        DataSet<WordCount> countDataSet = tableEnv.toDataSet(sqlQuery, WordCount.class);
        countDataSet.print();
    }
}
