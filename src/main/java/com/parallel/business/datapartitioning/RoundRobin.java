package com.parallel.business.datapartitioning;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Ismael Sadeghi, 2020-01-06 20:03
 */
public class RoundRobin<T> extends DataPartitioning<T> {

    public RoundRobin(List<T> data, int partitions) {
        super(data, partitions);
    }

    @Override
    public List<List<T>> getpartitions() {
        List<List<T>> result = new ArrayList<List<T>>();
        for (int i = 0; i < super.partitions; i++) {
            result.add(new ArrayList<T>());
        }
        int size = super.data.size();
        int count = 0;

        whileloop: while (true) {
            for (int i = 0; i < super.partitions; i++) {
                result.get(i).add(super.data.get(count++));
                if (count == size) {
                    break whileloop;
                }
            }
        }

        return result;
    }
}
