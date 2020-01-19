package com.distributedDatabase.data;

import lombok.Data;

import java.util.List;

@Data
public class Ram<T> {
    private int size;
    private List<T> buffer;
}
