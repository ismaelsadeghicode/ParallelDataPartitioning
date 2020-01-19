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

public class ParallelRedistributionBinaryMergeService<T> extends ParallelSortService<T> {
    private static final Logger LOG = Logger.getLogger(ParallelMergeAllService.class.getName());
    private long timePhaseLocalSort;
    private long timePhaseBinaryRedistribute;

    public ParallelRedistributionBinaryMergeService(Storage<T> storage, List<Cpu<T>> cpus, Comparator<? super T> comparator) {
        super(storage, cpus, comparator);
    }

    @Override
    public List<T> doSort() throws IOException {

        try {
            List<T> result = new ArrayList<>();
            phaseRoundRobin();
            cpusHistories.put("prr", cloneCpus(cpus));

            phaseLocalSort();
            cpusHistories.put("pls", cloneCpus(cpus));

            phaseBinaryRedistribute();
            cpusHistories.put("pbr", cloneCpus(cpus));

            phaseLocalSort();
            cpusHistories.put("pbrs", cloneCpus(cpus));

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
        RoundRobinService<T> roundRobin = new RoundRobinService<>(storage.getBigData(), cpus.size());
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

    private void phaseBinaryRedistribute() {
        LOG.log(Level.INFO, "Redistributing Started.");
        timePhaseBinaryRedistribute = System.currentTimeMillis();
        for (int i = 0; i < cpus.size(); i += 2) {
            List<Cpu<T>> tempcpus = new ArrayList<Cpu<T>>();
            tempcpus.add(cpus.get(i));
            tempcpus.add(cpus.get(i + 1));
            new RedistributeService<>(tempcpus).doRedistribute();
        }
        timePhaseBinaryRedistribute = (System.currentTimeMillis() - timePhaseBinaryRedistribute);
        LOG.log(Level.INFO, "Redistributing Ended.");
    }

    private void phaseRedistribute() {
        LOG.log(Level.INFO, "Redistributing Started.");
        timePhaseBinaryRedistribute = System.currentTimeMillis();
        new RedistributeService<>(cpus).doRedistribute();
        timePhaseBinaryRedistribute = (System.currentTimeMillis() - timePhaseBinaryRedistribute);
        LOG.log(Level.INFO, "Redistributing Ended.");
    }

}
