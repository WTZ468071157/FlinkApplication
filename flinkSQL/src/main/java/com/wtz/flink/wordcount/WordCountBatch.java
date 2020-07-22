package com.wtz.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author tiezhu
 * Date 2020/7/19 周日
 * Company dtstack
 */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        // 创建运行时环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 构建数据源
        DataSet<String> text = env.readTextFile("/Users/wtz4680/Desktop/ideaProject/flinkApplicationLearn/flnkSQL/src/main/resources/wordCount.txt");

        DataSet<Tuple2<String, Integer>> count = text.flatMap(new LineSplitter()).groupBy(0).sum(1);
        // 不建议在Flink中使用lambda表达式，下面的案例会报错，但是是正常的lambda表达式使用
//        DataSet<Tuple2<String, Integer>> count2 = text.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
//                    String[] tokens = value.toLowerCase().split("\\W+");
//                    for (String item : tokens) {
//                        if (item.length() > 0) {
//                            out.collect(new Tuple2<>(item, 1));
//                        }
//                    }
//                }
//        ).groupBy(0).sum(1);

        count.printToErr();
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //  将文本内容分割
            String[] tokens = value.toLowerCase().split("\\W+");

            for (String item : tokens) {
                if (item.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(item, 1));
                }
            }
        }
    }
}
