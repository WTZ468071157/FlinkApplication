package com.wtz.flink.sql;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author tiezhu
 * Date 2020/7/19 周日
 * Company dtstack
 */
public class MyStreamSource implements SourceFunction<Item> {

    private static Boolean isRunning = true;

    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(generateItem());

            // 等待一段时间后再生成数据
            Thread.sleep(TimeUnit.SECONDS.toSeconds(3));
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    //随机产生一条商品数据
    private Item generateItem() {
        int i = new Random().nextInt(100);
        ArrayList<String> list = new ArrayList<>();
        list.add("HAT");
        list.add("TIE");
        list.add("SHOE");
        Item item = new Item();
        item.setName(list.get(new Random().nextInt(3)));
        item.setId(i);
        return item;
    }
}
