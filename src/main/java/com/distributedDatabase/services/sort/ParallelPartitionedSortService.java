package com.distributedDatabase.services.sort;

import com.distributedDatabase.data.Cpu;
import com.distributedDatabase.data.Storage;
import com.distributedDatabase.services.datapartitioning.RedistributeService;
import com.distributedDatabase.services.datapartitioning.RoundRobinService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ParallelPartitionedSortService<T> extends ParallelSortService<T> {
    private static final Logger LOG = Logger.getLogger(ParallelMergeAllService.class.getName());
    private long timePhaseLocalSort;
    private long timePhaseBinaryRedistribute;

    public ParallelPartitionedSortService(Storage<T> storage, List<Cpu<T>> cpus, Comparator<? super T> comparator) {
        super(storage, cpus, comparator);
    }

    @Override
    public List<T> doSort() throws IOException {

        try {
            List<T> result = new ArrayList<>();
            phaseRoundRobin();
            cpusHistories.put("prr", cloneCpus(cpus));

            phaseRedistribute();
            cpusHistories.put("pr", cloneCpus(cpus));

            phaseLocalSort();
            cpusHistories.put("prs", cloneCpus(cpus));

            for (Cpu<T> cpu : cpus) {
                for (T t : cpu.getBuffer()) {
                    result.add(t);
                }
            }

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
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

    private void phaseRedistribute() {
        LOG.log(Level.INFO, "Redistributing Started.");
        timePhaseBinaryRedistribute = System.currentTimeMillis();
        new RedistributeService<>(cpus).doRedistribute();
        timePhaseBinaryRedistribute = (System.currentTimeMillis() - timePhaseBinaryRedistribute);
        LOG.log(Level.INFO, "Redistributing Ended.");
    }

}
