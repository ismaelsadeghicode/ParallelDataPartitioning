package com.distributedDatabase.data;

import lombok.Data;

import java.util.List;

@Data
public class BusRamCpu<T> {
    private int recordSize;
    private List<Object> buffer;
    private Ram<T> ram;
    private Cpu<T> cpu;
}
