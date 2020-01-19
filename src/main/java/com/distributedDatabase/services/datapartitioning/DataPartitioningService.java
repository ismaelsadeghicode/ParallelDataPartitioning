package com.distributedDatabase.services.datapartitioning;

import java.util.List;

public abstract class DataPartitioningService<T> {
    protected List<T> data;
    protected int partitions;

    public DataPartitioningService(List<T> data, int partitions) {
        this.data = data;
        this.partitions = partitions;
    }

    public abstract List<? extends List<T>> getpartitions();
}
