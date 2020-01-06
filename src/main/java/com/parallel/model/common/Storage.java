package com.parallel.model.common;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Ismael Sadeghi, 2020-01-06 19:43
 */
public class Storage<T> {
    private List<T> bigData;
    private List<T> sortedData;
    private List<T> data;

    public List<T> getBigData() {
        return bigData;
    }

    public void setBigData(List<T> bigData) {
        this.bigData = bigData;
    }

    public List<T> getSortedData() {
        return sortedData;
    }

    public void setSortedData(List<T> sortedData) {
        this.sortedData = sortedData;
    }

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public void save(T t) {
        data.add(t);
    }

    public void delete(T t) {
        data.remove(t);
    }

    public void bigDataSaveTofile() throws IOException {
        FileWriter writer = new FileWriter("bigdata_" + System.currentTimeMillis() + ".csv");
        String collect = bigData.stream().map(Object::toString).collect(Collectors.joining(","));
        writer.write(collect);
        writer.close();
    }

    public void dataSaveTofile() throws IOException {
        FileWriter writer = new FileWriter("data_" + System.currentTimeMillis() + ".csv");
        String collect = data.stream().map(Object::toString).collect(Collectors.joining(","));
        writer.write(collect);
        writer.close();
    }
}
