package com.wtz.flink.sql;

/**
 * @author tiezhu
 * Date 2020/7/19 周日
 * Company dtstack
 */
public class Item {
    public String name;
    public int id;

    public Item() {
    }

    public Item(String name, int id) {
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Item{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';
    }
}
