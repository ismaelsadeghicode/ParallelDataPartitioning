package com.parallel.business.sort;

import com.parallel.business.datapartitioning.RoundRobin;
import com.parallel.model.common.CPU;
import com.parallel.model.parallelmergall.Storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author Ismael Sadeghi, 2020-01-06 19:38
 */
public class ParallelBinaryMerge<T> extends ParallelSort<T> {
    private static final Logger LOG = Logger.getLogger(ParallelMergeAll.class.getName());
    private List<List<CPU<T>>> allStepsCpus;
    private long[] timePhaseLocalSort;
    private long[] timePhaseFinalMerge;

    public ParallelBinaryMerge(Storage<T> storage, List<CPU<T>> cpus, Comparator<? super T> comparator) {
        super(storage, cpus, comparator);
        allStepsCpus = getCpuSteps(cpus.size());
        timePhaseLocalSort = new long[allStepsCpus.size()];
        timePhaseFinalMerge = new long[allStepsCpus.size()];
    }

    public List<List<CPU<T>>> getAllStepsCpus() {
        return allStepsCpus;
    }

    @Override
    public List<T> doSort() throws IOException {

        try {
            List<T> result;
            cpus = allStepsCpus.get(0);
            phaseRoundRobin();
            cpusHistories.put("prr", cloneCpus(cpus));

            int step = 0;
            while (true) {
                cpus = cpus.parallelStream().filter(cpu -> !cpu.getBuffer().isEmpty()).collect(Collectors.toList());
                int cpusCount = cpus.size();
                if (cpusCount == 2) {
                    phaseMerge(step, cpus);
                    cpusHistories.put("pm" + step, cloneCpus(cpus));
                    phaseLocalSort(step, cpus);
                    cpusHistories.put("pls" + step, cloneCpus(cpus));
                    result = cpus.get(0).getBuffer();
                    break;
                }

                phaseLocalSort(step, cpus);
                cpusHistories.put("pls" + step, cloneCpus(cpus));

                for (int i = 0; i < cpusCount; i += 2) {
                    List<CPU<T>> tempcpus = new ArrayList<CPU<T>>();
                    tempcpus.add(cpus.get(i));
                    tempcpus.add(cpus.get(i + 1));
                    phaseMerge(step, tempcpus);
                }
                cpusHistories.put("pm" + step, cloneCpus(cpus));
                step++;
            }

            return cpusHistories.get("pm" + step).get(0).getBuffer();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void phaseRoundRobin() throws IOException {
        LOG.log(Level.INFO, "Start RoundRobin");
        timeRoundRobin = System.currentTimeMillis();
        RoundRobin<T> roundRobin = new RoundRobin<T>(storage.getBigData(), cpus.size());
        List<List<T>> result = roundRobin.getpartitions();

        int c = 0;
        for (CPU<T> cpu : cpus) {
            cpu.setBuffer(result.get(c++));
        }

        timeRoundRobin = (System.currentTimeMillis() - timeRoundRobin);
        LOG.log(Level.INFO, "End of RoundRobin");
    }

    private void phaseLocalSort(int step, List<CPU<T>> cpus) {
        LOG.log(Level.INFO, "Local Sorting Started.");
        timePhaseLocalSort[step] = System.currentTimeMillis();
        for (CPU<T> cpu : cpus) {
            // while (cpu.isAlive())
            // ;
            cpu.sort(comparator);
        }
        timePhaseLocalSort[step] = (System.currentTimeMillis() - timePhaseLocalSort[step]);
        LOG.log(Level.INFO, "Local Sorting Ended.");
    }

    private List<T> phaseMerge(int step, List<CPU<T>> cpus) {
        LOG.log(Level.INFO, "Start Merging");
        timePhaseFinalMerge[step] = System.currentTimeMillis();
        cpus.get(0).mergeAll_ParallelMergeAll(cpus, comparator);
        for (int i = 1; i < cpus.size(); i++) {
            cpus.get(i).setBuffer(new ArrayList<T>());
        }
        timePhaseFinalMerge[step] = (System.currentTimeMillis() - timePhaseFinalMerge[step]);
        LOG.log(Level.INFO, "End Merging");
        return cpus.get(0).getBuffer();
    }

    public boolean isBinaryNumber(int no) {
        return Integer.toBinaryString(no - 1).replace("1", "").isEmpty();
    }

    public List<List<CPU<T>>> getCpuSteps(int no) {
        if (!isBinaryNumber(no)) {
            return null;
        }
        List<List<CPU<T>>> result = new ArrayList<>();

        result.add(cpus);
        while (no != 1) {
            no = no / 2;
            List<CPU<T>> cpuList = new ArrayList<CPU<T>>();
            for (int i = 1; i <= no; i++) {
                CPU<T> cpu = new CPU<>();
                cpu.setCpuName("cpu_" + i);
                cpuList.add(cpu);
            }
            result.add(cpuList);
        }
        return result;
    }

}
