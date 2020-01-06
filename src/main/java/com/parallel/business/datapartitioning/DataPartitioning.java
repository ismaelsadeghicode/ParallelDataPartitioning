package com.parallel.business.datapartitioning;

import java.util.List;

/**
 * @author Ismael Sadeghi, 2020-01-06 20:03
 */
public abstract class DataPartitioning<T> {
    protected List<T> data;
    protected int partitions;

    public DataPartitioning(List<T> data, int partitions) {
        this.data = data;
        this.partitions = partitions;
    }

    public abstract List<? extends List<T>> getpartitions();
}
