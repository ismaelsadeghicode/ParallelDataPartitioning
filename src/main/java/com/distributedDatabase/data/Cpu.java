package com.distributedDatabase.data;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Cpu<T> extends Thread implements Cloneable {
    private String cpuName;
    private EnumStatus status;
    private List<T> buffer;
    private List<T> temporary;
    private Comparator<? super T> comparator;
    private long redistributemin;
    private long redistributemax;
    private long numberOfActivities;
    private long activityRoundRobin;
    private long activityLocalSort;
    private long activityFinalMerge;
    private long activityRedistribute;

    public Cpu() {
        buffer = new ArrayList<>();
        temporary = new ArrayList<>();
    }

    public Cpu(String cpuName, List<T> buffer, List<T> temporary, Comparator<? super T> comparator,
               long activityRoundRobin, long numberOfActivities, long activityLocalSort, long activityFinalMerge) {
        super();
        this.cpuName = cpuName;
        this.buffer = buffer;
        this.temporary = temporary;
        this.comparator = comparator;
        this.numberOfActivities = numberOfActivities;
        this.activityRoundRobin = activityRoundRobin;
        this.activityLocalSort = activityLocalSort;
        this.activityFinalMerge = activityFinalMerge;
    }

    public String getCpuName() {
        return cpuName;
    }

    public void setCpuName(String cpuName) {
        this.cpuName = cpuName;
    }

    public long getRedistributemin() {
        return redistributemin;
    }

    public void setRedistributemin(long redistributemin) {
        this.redistributemin = redistributemin;
    }

    public long getRedistributemax() {
        return redistributemax;
    }

    public void setRedistributemax(long redistributemax) {
        this.redistributemax = redistributemax;
    }

    public long getNumberOfActivities() {
        return numberOfActivities;
    }

    public long getActivityRoundRobin() {
        return activityRoundRobin;
    }

    public long getActivityLocalSort() {
        return activityLocalSort;
    }

    public long getActivityFinalMerge() {
        return activityFinalMerge;
    }

    public long getActivityRedistribute() {
        return activityRedistribute;
    }

    public void setActivityRedistribute(long activityRedistribute) {
        this.activityRedistribute = activityRedistribute;
    }

    public void pluseActivityRedistribute() {
        this.activityRedistribute++;
    }

    public List<T> getBuffer() {
        return buffer;
    }

    public void setBuffer(List<T> buffer) {
        status = EnumStatus.SET_BUFFER;
        numberOfActivities += buffer.size();
        activityRoundRobin += buffer.size();
        // super.start();
        this.buffer = buffer;
    }

    public List<T> getTemporary() {
        return temporary;
    }

    public void setTemporary(List<T> temporary) {
        status = EnumStatus.SET_TEMPORARY;
        numberOfActivities += temporary.size();
        activityRedistribute += temporary.size();
        this.temporary = temporary;
    }

    public Comparator<? super T> getComparator() {
        return comparator;
    }

    public Cpu<T> sort(Comparator<? super T> comparator) {
        status = EnumStatus.SORT;
        this.comparator = comparator;
        activityLocalSort += buffer.size();
        numberOfActivities += buffer.size();
        buffer.sort(comparator);
        // start();
        return this;
    }

    public void saveBuffre() {
        status = EnumStatus.SAVE_BUFFER;
        try {
            FileWriter writer;
            writer = new FileWriter("CPU_BUFFER_" + cpuName + "_" + System.currentTimeMillis() + ".csv");
            String collect = buffer.stream().map(Object::toString).collect(Collectors.joining(","));
            writer.write(collect);
            writer.close();
        } catch (Exception e) {

        }
        // start();
    }

    public void saveTemporary() {
        status = EnumStatus.SAVE_TEMPORARY;
        try {
            FileWriter writer;
            writer = new FileWriter("CPU_BUFFER_" + cpuName + "_" + System.currentTimeMillis() + ".csv");
            String collect = temporary.stream().map(Object::toString).collect(Collectors.joining(","));
            writer.write(collect);
            writer.close();
        } catch (Exception e) {

        }
        // start();
    }

    public List<T> mergeAll_ParallelMergeAll(List<Cpu<T>> cpus, Comparator<? super T> comparator) {
        List<T> result = new ArrayList<T>();

        int max = cpus.stream().max((x, y) -> x.buffer.size() - y.buffer.size()).get().buffer.size();
        for (int i = 0; i < max; i++) {
            for (Cpu<T> cpu : cpus) {
                try {
                    result.add(cpu.getBuffer().get(i));
                    numberOfActivities++;
                    activityFinalMerge++;
                } catch (Exception e) {
                }
            }
        }
        buffer = result = result.parallelStream().sorted(comparator).collect(Collectors.toList());
        return result;
    }

    @Override
    public void run() {
        this.setName(cpuName);
        try {
            switch (status) {
                case SET_BUFFER:
                    // Performs buffering commands
                    break;
                case SET_TEMPORARY:
                    // Performs temporaring commands
                    break;
                case SORT:
                    buffer.sort(comparator);
                    break;
                case SAVE_BUFFER: {
                    synchronized (this) {
                        FileWriter writer;
                        writer = new FileWriter("CPU_BUFFER_" + cpuName + "_" + System.currentTimeMillis() + ".csv");
                        String collect = buffer.stream().map(Object::toString).collect(Collectors.joining(","));
                        writer.write(collect);
                        writer.close();
                    }
                    break;
                }
                case SAVE_TEMPORARY: {
                    synchronized (this) {
                        FileWriter writer = new FileWriter(
                                "CPU_TEMPORARY_" + cpuName + "_" + System.currentTimeMillis() + ".csv");
                        String collect = temporary.stream().map(Object::toString).collect(Collectors.joining(","));
                        writer.write(collect);
                        writer.close();
                    }
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}