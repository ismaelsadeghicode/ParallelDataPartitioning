package com.distributedDatabase.services.sort;

import com.distributedDatabase.data.Cpu;
import com.distributedDatabase.data.Storage;

import java.io.IOException;
import java.util.*;

public abstract class ParallelSortService<T> {
    protected long timeRoundRobin;

    protected Storage<T> storage;
    protected List<Cpu<T>> cpus;
    protected Map<String, List<Cpu<T>>> cpusHistories;
    protected Comparator<? super T> comparator;

    public ParallelSortService(Storage<T> storage, List<Cpu<T>> cpus, Comparator<? super T> comparator) {
        this.storage = storage;
        this.cpus = cpus;
        this.comparator = comparator;
        cpusHistories = new HashMap<String, List<Cpu<T>>>();
    }

    public long getTimeRoundRobin() {
        return timeRoundRobin;
    }

    public Map<String, List<Cpu<T>>> getCpusHistories() {
        return cpusHistories;
    }

    public abstract List<T> doSort() throws IOException;

    protected List<Cpu<T>> cloneCpus(List<Cpu<T>> cpus) {
        List<Cpu<T>> result = new ArrayList<Cpu<T>>();
        for (Cpu<T> cpu : cpus) {
            Cpu<T> temp = new Cpu<>(cpu.getCpuName(), new ArrayList<>(cpu.getBuffer()),
                    new ArrayList<>(cpu.getTemporary()), cpu.getComparator(), cpu.getActivityRoundRobin(),
                    cpu.getNumberOfActivities(), cpu.getActivityLocalSort(), cpu.getActivityFinalMerge());
            temp.setActivityRedistribute(cpu.getActivityRedistribute());
            result.add(temp);
        }

        return result;
    }
}
