package com.distributedDatabase.data;

import lombok.Data;

import java.util.List;

@Data
public class BusStorageRam<T> {
    private int pageSize;
    private List<T> buffer;
    private Storage<T> storage;
    private Ram<T> ram;
}
