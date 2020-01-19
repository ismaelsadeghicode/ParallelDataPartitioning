package com.distributedDatabase.data;

import lombok.Data;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class Storage<T> {
    private List<T> bigData;
    private List<T> sortedData;
    private List<T> data;

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
