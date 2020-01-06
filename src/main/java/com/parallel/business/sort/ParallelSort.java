package com.parallel.business.sort;

import com.parallel.model.common.CPU;
import com.parallel.model.parallelmergall.Storage;

import java.io.IOException;
import java.util.*;

/**
 * @author Ismael Sadeghi, 2020-01-06 19:40
 */
public abstract class ParallelSort<T> {
    protected long timeRoundRobin;

    protected Storage<T> storage;
    protected List<CPU<T>> cpus;
    protected Map<String, List<CPU<T>>> cpusHistories;
    protected Comparator<? super T> comparator;

    public ParallelSort(Storage<T> storage, List<CPU<T>> cpus, Comparator<? super T> comparator) {
        this.storage = storage;
        this.cpus = cpus;
        this.comparator = comparator;
        cpusHistories = new HashMap<String, List<CPU<T>>>();
    }

    public long getTimeRoundRobin() {
        return timeRoundRobin;
    }

    public Map<String, List<CPU<T>>> getCpusHistories() {
        return cpusHistories;
    }

    public abstract List<T> doSort() throws IOException;

    protected List<CPU<T>> cloneCpus(List<CPU<T>> cpus) {
        List<CPU<T>> result = new ArrayList<CPU<T>>();
        for (CPU<T> cpu : cpus) {
            CPU<T> temp = new CPU<>(cpu.getCpuName(), new ArrayList<>(cpu.getBuffer()),
                    new ArrayList<>(cpu.getTemporary()), cpu.getComparator(), cpu.getActivityRoundRobin(),
                    cpu.getNumberOfActivities(), cpu.getActivityLocalSort(), cpu.getActivityFinalMerge());
            temp.setActivityRedistribute(cpu.getActivityRedistribute());
            result.add(temp);
        }

        return result;
    }
}
