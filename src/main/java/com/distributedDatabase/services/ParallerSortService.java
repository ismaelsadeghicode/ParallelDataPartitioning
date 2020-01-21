package com.distributedDatabase.services;

import com.distributedDatabase.data.*;
import com.distributedDatabase.services.sort.*;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

@Component
public class ParallerSortService implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<Cpu<Integer>> cpus;
    private Storage<Integer> storage;

    private int cpuCount;
    private int noRandomGeneration;
    private String selectedAlgorithm = "";

    private ParallelMergeAllService<Integer> parallelMergeAll;
    private ParallelBinaryMergeService<Integer> parallelBinaryMerge;
    private ParallelRedistributionBinaryMergeService<Integer> parallelRedistributionBinaryMerge;
    private ParallelRedistributionMergeAllService<Integer> parallelRedistributionMergeAll;
    private ParallelPartitionedSortService<Integer> parallelPartitionedSort;

    public Map<Integer, String> listOfAlgorithms() {
        HashMap<Integer, String> algorithmList = new HashMap<>();
        algorithmList.put(1, "merge_all");
        algorithmList.put(2, "binary-merge");
        algorithmList.put(3, "redistribution_binary_merge");
        algorithmList.put(4, "redistribution_merge_all");
        algorithmList.put(5, "partitioned");
        return algorithmList;
    }

    public List<Cpu<Integer>> getCpus() {
        if (cpus == null) {
            cpus = new ArrayList<Cpu<Integer>>();
            for (int i = 1; i <= cpuCount; i++) {
                Cpu<Integer> cpu = new Cpu<Integer>();
                cpu.setCpuName("cpu_" + i);
                cpus.add(cpu);
            }
        }
        return cpus;
    }

    public Storage<Integer> getStorage() {
        if (storage == null) {
            storage = new Storage<Integer>();
        }
        if (storage.getBigData() == null) {
            storage.setBigData(new ArrayList<Integer>());
        }
        return storage;
    }


    public String getFullnameSelectedAlgorithm() {
        switch (selectedAlgorithm) {
            case "merge_all":
                return "Parallel Merge-All";
            case "binary_merge":
                return "Parallel Binary-Merge";
            case "redistribution_binary_merge":
                return "Parallel Redistribution Binary-Merge";
            case "redistribution_merge_all":
                return "Parallel Redistribution Merge-All";
            case "partitioned":
                return "Parallel Partitioned";
            default:
                return "";
        }
    }


    public String createTitle() {
        StringBuilder result = new StringBuilder();
        switch (selectedAlgorithm) {
            case "merge_all":
                result.append("[ 'Round Robin', 'Local Sort', 'Final Merge', 'Total Activity']");
                break;
            case "binary_merge":
                int sm = parallelBinaryMerge.getAllStepsCpus().size() - 1;
                result.append("[ 'Round Robin',");
                for (int i = 1; i <= sm; i++) {
                    result.append("'Sort Step ");
                    result.append(i);
                    result.append("', 'Merge Step ");
                    result.append(i);
                    result.append("',");
                }
                result.append("'Total Activity']");
                break;
            case "redistribution_binary_merge":
                result.append(
                        "[ 'Round Robin', 'Local Sort',  'Bin Redistribute', 'Local Sort',  'Final Redistribute', 'Local Sort', 'Total Activity']");
                break;
            case "redistribution_merge_all":
                result.append("[ 'Round Robin', 'Local Sort',  'Final Redistribute', 'Local Sort', 'Total Activity']");
                break;
            case "partitioned":
                result.append("[ 'Round Robin', 'Final Redistribute', 'Local Sort','Total Activity']");
                break;

            default:
                break;
        }
        return result.toString();
    }


    public Response parallelBinaryMerge(Request request) throws IOException {
        cpuCount = request.getCpuCount();
        noRandomGeneration = request.getRandomNO();
        selectedAlgorithm = request.getAlgoritmNo();

        Response response = new Response();
        if (cpuCount % 2 != 0) {
            response.setSuccessful(Boolean.FALSE);
            response.setErrorDescription("The number of Cpus in a system is always even");
            return response;
        }
        if (cpuCount > noRandomGeneration) {
            response.setSuccessful(Boolean.FALSE);
            response.setErrorDescription("Random numbers are less than Cpus");
            return response;
        }
        if (noRandomGeneration == 0) {
            noRandomGeneration = 1000000;
        }
        if (cpuCount == 0) {
            cpuCount = Runtime.getRuntime().availableProcessors();
        }
        if (selectedAlgorithm.contentEquals("binary_merge")) {
            if (!Integer.toBinaryString(cpuCount - 1).replace("1", "").isEmpty()) {
                response.setSuccessful(Boolean.FALSE);
                response.setErrorDescription("The number of Cpus is not suitable for your algorithm of choice");
                return response;
            }
        }

        reset();
        fillStorage();
        switch (selectedAlgorithm) {
            case "merge_all":
                parallelMergeAll = new ParallelMergeAllService<Integer>(getStorage(), getCpus(), (x, y) -> x.compareTo(y));
                getStorage().setSortedData(parallelMergeAll.doSort());
                break;
            case "binary_merge":
                parallelBinaryMerge = new ParallelBinaryMergeService<Integer>(getStorage(), getCpus(), (x, y) -> x.compareTo(y));
                getStorage().setSortedData(parallelBinaryMerge.doSort());
                break;
            case "redistribution_binary_merge":
                parallelRedistributionBinaryMerge = new ParallelRedistributionBinaryMergeService<Integer>(getStorage(), getCpus(),
                        (x, y) -> x.compareTo(y));
                getStorage().setSortedData(parallelRedistributionBinaryMerge.doSort());
                break;
            case "redistribution_merge_all":
                parallelRedistributionMergeAll = new ParallelRedistributionMergeAllService<Integer>(getStorage(), getCpus(),
                        (x, y) -> x.compareTo(y));
                getStorage().setSortedData(parallelRedistributionMergeAll.doSort());
                break;
            case "partitioned":
                parallelPartitionedSort = new ParallelPartitionedSortService<Integer>(getStorage(), getCpus(),
                        (x, y) -> x.compareTo(y));
                getStorage().setSortedData(parallelPartitionedSort.doSort());
                break;
            default:
                break;
        }

        List<ProcessResponse> result = new ArrayList<>();

        int c = 1;
        for (Cpu<Integer> cpu : cpus) {
            ProcessResponse processResponse = new ProcessResponse();
            processResponse.setProcess(cpu.getCpuName());
            switch (selectedAlgorithm) {
                case "merge_all":
                    DataProcessResponse data = new DataProcessResponse();
//                    data.setAlgorithm("merge_all");
                    data.setTitle(createTitle());
                    data.setData(getDataChartMergeAll(cpu));
                    processResponse.setDatas(data);
                    break;
                case "binary_merge":
                    DataProcessResponse data2 = new DataProcessResponse();
//                    data2.setAlgorithm("binary_merge");
                    data2.setTitle(createTitle());
                    data2.setData(getDataChartBinaryMerge(cpu));
                    processResponse.setDatas(data2);
                    break;
                case "redistribution_binary_merge":
                    DataProcessResponse data3 = new DataProcessResponse();
//                    data3.setAlgorithm("redistribution_binary_merge");
                    data3.setTitle(createTitle());
                    data3.setData(getDataRedistributionBinaryMerge(cpu));
                    processResponse.setDatas(data3);
                    break;
                case "redistribution_merge_all":
                    DataProcessResponse data4 = new DataProcessResponse();
//                    data4.setAlgorithm("redistribution_merge_all");
                    data4.setTitle(createTitle());
                    data4.setData(getDataRedistributionMergeAll(cpu));
                    processResponse.setDatas(data4);
                    break;
                case "partitioned":
                    DataProcessResponse data5 = new DataProcessResponse();
//                    data5.setAlgorithm("partitioned");
                    data5.setTitle(createTitle());
                    data5.setData(getDataPartitionedSort(cpu));
                    processResponse.setDatas(data5);
                    break;
            }
            c++;
            result.add(processResponse);
        }

        response.setAlgorithm(selectedAlgorithm);
        response.setSuccessful(Boolean.TRUE);
        response.setResponse(result);

        return response;
    }

    private void fillStorage() {
        for (int i = 0; i < noRandomGeneration; i++) {
            getStorage().getBigData().add(Math.abs(new Random().nextInt()));
        }
    }

    private void reset() {
        cpus = null;
        storage = null;
    }

    private String getDataChartMergeAll(Cpu<Integer> cpu) {
        StringBuilder result = new StringBuilder();
        // [ 'Round Robin', 'Local Sort', 'Final Merge', 'Total Activity']
        result.append(cpu.getActivityRoundRobin());
        result.append(",");
        result.append(cpu.getActivityLocalSort());
        result.append(",");
        result.append(cpu.getActivityFinalMerge());
        result.append(",");
        result.append(cpu.getNumberOfActivities());
        return result.toString();
    }

    private String getDataChartBinaryMerge(Cpu<Integer> cpu) {
        StringBuilder result = new StringBuilder();
        // [ 'Round Robin', 'Local Sort', 'Final Merge', ... , 'Total Activity']

        result.append(cpu.getActivityRoundRobin());
        result.append(",");
        Map<String, List<Cpu<Integer>>> allStep = parallelBinaryMerge.getCpusHistories();
        int sm = parallelBinaryMerge.getAllStepsCpus().size() - 1;
        for (int i = 0; i < sm; i++) {
            String ps = "pls" + i;
            String pm = "pm" + i;
            Cpu<Integer> cpups = allStep.get(ps).parallelStream().filter(c -> c.getCpuName().equals(cpu.getCpuName()))
                    .findAny().orElse(new Cpu<>());
            Cpu<Integer> cpupm = allStep.get(pm).parallelStream().filter(c -> c.getCpuName().equals(cpu.getCpuName()))
                    .findAny().orElse(new Cpu<>());
            result.append(cpups.getActivityLocalSort());
            result.append(",");
            result.append(cpupm.getActivityFinalMerge());
            result.append(",");
        }
        result.append(cpu.getNumberOfActivities());

        return result.toString();
    }

    private String getDataRedistributionBinaryMerge(Cpu<Integer> cpu) {
        StringBuilder result = new StringBuilder();
        // [ 'Round Robin', 'Local Sort', 'Bin Redistribute', 'Local Sort', 'Final
        // Redistribute', 'Local Sort', 'Total Activity']

        Cpu<Integer> temp = parallelRedistributionBinaryMerge.getCpusHistories().get("prr").parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        result.append(temp.getActivityRoundRobin());
        result.append(",");

        temp = parallelRedistributionBinaryMerge.getCpusHistories().get("pls").parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        result.append(temp.getActivityLocalSort());
        result.append(",");

        temp = parallelRedistributionBinaryMerge.getCpusHistories().get("pbr").parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        result.append(temp.getActivityRedistribute());
        result.append(",");

        temp = parallelRedistributionBinaryMerge.getCpusHistories().get("pbrs").parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        result.append(temp.getActivityLocalSort());
        result.append(",");

        temp = parallelRedistributionBinaryMerge.getCpusHistories().get("pr").parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        result.append(cpu.getActivityRedistribute());
        result.append(",");

        temp = parallelRedistributionBinaryMerge.getCpusHistories().get("prs").parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        result.append(cpu.getActivityRedistribute());
        result.append(",");

        result.append(cpu.getNumberOfActivities());
        return result.toString();
    }

    private String getDataRedistributionMergeAll(Cpu<Integer> cpu) {
        StringBuilder result = new StringBuilder();
        // [ 'Round Robin', 'Local Sort', 'Bin Redistribute', 'Local Sort', 'Final
        // Redistribute', 'Local Sort', 'Total Activity']

        Cpu<Integer> temp = parallelRedistributionMergeAll.getCpusHistories().get("prr").parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        result.append(temp.getActivityRoundRobin());
        result.append(",");

        temp = parallelRedistributionMergeAll.getCpusHistories().get("pls").parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        result.append(temp.getActivityLocalSort());
        result.append(",");

        temp = parallelRedistributionMergeAll.getCpusHistories().get("pr").parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        result.append(cpu.getActivityRedistribute());
        result.append(",");

        temp = parallelRedistributionMergeAll.getCpusHistories().get("prs").parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        result.append(cpu.getActivityRedistribute());
        result.append(",");

        result.append(cpu.getNumberOfActivities());
        return result.toString();
    }

    private String getDataPartitionedSort(Cpu<Integer> cpu) {
        StringBuilder result = new StringBuilder();
        // [ 'Round Robin', 'Local Sort', 'Bin Redistribute', 'Local Sort', 'Final
        // Redistribute', 'Local Sort', 'Total Activity']

        Cpu<Integer> temp = parallelPartitionedSort.getCpusHistories().get("prr").parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        result.append(temp.getActivityRoundRobin());
        result.append(",");

        temp = parallelPartitionedSort.getCpusHistories().get("pr").parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        result.append(cpu.getActivityRedistribute());
        result.append(",");

        temp = parallelPartitionedSort.getCpusHistories().get("prs").parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        result.append(cpu.getActivityRedistribute());
        result.append(",");

        result.append(cpu.getNumberOfActivities());
        return result.toString();
    }

    public String getStyleBtnDownloadParallelBinaryMergePS(Cpu<Integer> cpu, int step) {
        String pls = "pls" + step;
        Cpu<Integer> cpupls = parallelBinaryMerge.getCpusHistories().get(pls).parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        return cpupls.getBuffer().isEmpty() ? "btn btn-outline-warning" : "btn btn-info";
    }

    public String getStyleBtnDownloadParallelBinaryMergePM(Cpu<Integer> cpu, int step) {
        String pm = "pm" + step;
        Cpu<Integer> cpupls = parallelBinaryMerge.getCpusHistories().get(pm).parallelStream()
                .filter(c -> c.getCpuName().equals(cpu.getCpuName())).findAny().orElse(new Cpu<>());
        return cpupls.getBuffer().isEmpty() ? "btn btn-outline-warning" : "btn btn-info";
    }

    public ParallelMergeAllService<Integer> getParallelMergeAll() {
        if (parallelMergeAll == null) {
            parallelMergeAll = new ParallelMergeAllService<Integer>(getStorage(), getCpus(), (x, y) -> x.compareTo(y));
        }
        return parallelMergeAll;
    }

    public ParallelBinaryMergeService<Integer> getParallelBinaryMerge() {
        if (parallelBinaryMerge == null) {
            parallelBinaryMerge = new ParallelBinaryMergeService<Integer>(getStorage(), getCpus(), (x, y) -> x.compareTo(y));
        }
        return parallelBinaryMerge;
    }

    public ParallelRedistributionBinaryMergeService<Integer> getParallelRedistributionBinaryMerge() {
        if (parallelRedistributionBinaryMerge == null) {
            parallelRedistributionBinaryMerge = new ParallelRedistributionBinaryMergeService<Integer>(getStorage(), getCpus(),
                    (x, y) -> x.compareTo(y));
        }
        return parallelRedistributionBinaryMerge;
    }

    public ParallelRedistributionMergeAllService<Integer> getParallelRedistributionMergeAll() {
        if (parallelRedistributionMergeAll == null) {
            parallelRedistributionMergeAll = new ParallelRedistributionMergeAllService<Integer>(getStorage(), getCpus(),
                    (x, y) -> x.compareTo(y));
        }
        return parallelRedistributionMergeAll;
    }

    public ParallelPartitionedSortService<Integer> getParallelPartitionedSort() {
        if (parallelPartitionedSort == null) {
            parallelPartitionedSort = new ParallelPartitionedSortService<Integer>(getStorage(), getCpus(),
                    (x, y) -> x.compareTo(y));
        }
        return parallelPartitionedSort;
    }
}
