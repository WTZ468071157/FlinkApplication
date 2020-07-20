package com.wtz.flink.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tiezhu
 * Date 2020/7/19 周日
 * Company dtstack
 */
public class FlinkSQLDemo {
    public static void main(String[] args) throws Exception {
        // 创建流式table运行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        SingleOutputStreamOperator<Item> streamOperator = environment.addSource(new MyStreamSource())
                .map((MapFunction<Item, Item>) value -> value);

        DataStream<Item> evenSelect = getSelect(streamOperator, "even");
        DataStream<Item> oddSelect = getSelect(streamOperator, "odd");

        tableEnvironment.createTemporaryView("evenTable", evenSelect, "name, id");
        tableEnvironment.createTemporaryView("oddTable", oddSelect, "name, id");

        Table sqlQuery = tableEnvironment.
                sqlQuery("SELECT a.id AS aid, b.id AS bid, a.name AS aname, b.name AS bname FROM evenTable AS a JOIN oddTable AS b ON a.name = b.name");
        sqlQuery.printSchema();

        tableEnvironment.toRetractStream(sqlQuery, TypeInformation.of(new TypeHint<Tuple4<Integer, Integer, String, String>>() {
        })).print();

        environment.execute("table SQL");
    }

    private static DataStream<Item> getSelect(SingleOutputStreamOperator<Item> operator, String item) {
        return operator.split((OutputSelector<Item>) value -> {
            List<String> output = new ArrayList<>();
            if (value.getId() % 2 == 0) {
                output.add("even");
            } else {
                output.add("odd");
            }
            return output;
        }).select(item);
    }
}
