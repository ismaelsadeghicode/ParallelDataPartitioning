package com.distributedDatabase.services.sort;

import com.distributedDatabase.data.Cpu;
import com.distributedDatabase.data.Storage;
import com.distributedDatabase.services.datapartitioning.RoundRobinService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ParallelMergeAllService<T> extends ParallelSortService<T> {
    private static final Logger LOG = Logger.getLogger(ParallelMergeAllService.class.getName());

    private long timePhaseLocalSort;
    private long timePhaseFinalMerge;

    public ParallelMergeAllService(Storage<T> storage, List<Cpu<T>> cpus, Comparator<? super T> comparator) {
        super(storage, cpus, comparator);
    }

    private void phaseRoundRobin() throws IOException {
        LOG.log(Level.INFO, "Start RoundRobin");
        timeRoundRobin = System.currentTimeMillis();
        RoundRobinService<T> roundRobin = new RoundRobinService<T>(storage.getBigData(), cpus.size());
        List<List<T>> result = roundRobin.getpartitions();

        int c = 0;
        for (Cpu<T> cpu : cpus) {
            cpu.setBuffer(result.get(c++));
        }
        timeRoundRobin = (System.currentTimeMillis() - timeRoundRobin);
        LOG.log(Level.INFO, "End of RoundRobin");
    }

    private void phaseLocalSort() {
        LOG.log(Level.INFO, "Local Sorting Started.");
        timePhaseLocalSort = System.currentTimeMillis();
        for (Cpu<T> cpu : cpus) {
            cpu.sort(comparator);
        }
        timePhaseLocalSort = (System.currentTimeMillis() - timePhaseLocalSort);
        LOG.log(Level.INFO, "Local Sorting Ended.");
    }

    private List<T> phaseFinalMerge() {
        LOG.log(Level.INFO, "Start Merging");
        timePhaseFinalMerge = System.currentTimeMillis();
        cpus.get(0).mergeAll_ParallelMergeAll(cpus, comparator);
        for (int i = 1; i < cpus.size(); i++) {
            cpus.get(i).setBuffer(new ArrayList<T>());
        }
        timePhaseFinalMerge = (System.currentTimeMillis() - timePhaseFinalMerge);
        LOG.log(Level.INFO, "End Merging");
        return cpus.get(0).getBuffer();
    }

    @Override
    public List<T> doSort() throws IOException {
        try {
            phaseRoundRobin();
            cpusHistories.put("prr", cloneCpus(cpus));
            phaseLocalSort();
            cpusHistories.put("pls", cloneCpus(cpus));
            List<T> result = phaseFinalMerge();
            cpusHistories.put("pfm", cloneCpus(cpus));
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public long getTimePhaseFinalMerge() {
        return timePhaseFinalMerge;
    }

    public long getTimePhaseLocalSort() {
        return timePhaseLocalSort;
    }

}
