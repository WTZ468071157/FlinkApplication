package com.wtz.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author tiezhu
 * Date 2020/7/19 周日
 * Company dtstack
 */
public class WordCountStream {
    public static class WordCount {
        public String word;
        public Integer count;

        // 必须加上，不然会抛出
        // This type (GenericType<com.wtz.flink.wordcount.WordCountStream.WordCount>) cannot be used as key
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
        // 创建流式运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 监听9090端口
        DataStream<String> text = env.socketTextStream("localhost", 9090, "\n");

        // 将接受到的数据进行拆分，组合，窗口计算然后聚合输出
        DataStream<WordCount> counts = text.flatMap(new wordCountStreamFlatMap())
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce((ReduceFunction<WordCount>) (value1, value2)
                        -> new WordCount(value1.word, value1.count + value2.count));

        // 另一种写法
        //        DataStream<WordCount> counts = text.flatMap(new FlatMapFunction<String, WordCount>() {
//            @Override
//            public void flatMap(String value, Collector<WordCount> out) throws Exception {
//                for (String word : value.split("\\s")) {
//                    out.collect(new WordCount(word, 1));
//                }
//            }
//        }).keyBy("word")
//                .timeWindow(Time.seconds(5), Time.seconds(1))
//                .reduce(new ReduceFunction<WordCount>() {
//                    @Override
//                    public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
//                        return new WordCount(value1.word, value1.count + value2.count);
//                    }
//                });

        counts.print();

        env.execute("wordCount Stream");
    }

    public static class wordCountStreamFlatMap implements FlatMapFunction<String, WordCount> {
        @Override
        public void flatMap(String value, Collector<WordCount> out) throws Exception {
            for (String item : value.split("\\s")) {
                out.collect(new WordCount(item, 1));
            }
        }
    }
}
