package com.distributedDatabase.controller;

import com.distributedDatabase.data.Cpu;
import com.distributedDatabase.data.Request;
import com.distributedDatabase.data.Response;
import com.distributedDatabase.data.Storage;
import com.distributedDatabase.services.sort.*;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("parallel-sort")
public class ParallelSortController {

    private ParallelMergeAllService<Integer> parallelMergeAll;
    private ParallelBinaryMergeService<Integer> parallelBinaryMerge;
    private ParallelRedistributionBinaryMergeService<Integer> parallelRedistributionBinaryMerge;
    private ParallelRedistributionMergeAllService<Integer> parallelRedistributionMergeAll;
    private ParallelPartitionedSortService<Integer> parallelPartitionedSort;
    private Storage<Integer> storage;
    private List<Cpu<Integer>> cpus;
    private int cpuCount;
    private int noRandomGeneration;
    private String selectedAlgorithm = "";

    @GetMapping
    public Map<Integer, String> listOfAlgorithms() {
        HashMap<Integer, String> algorithmList = new HashMap<>();
        algorithmList.put(1, "merge_all");
        algorithmList.put(2, "binary-merge");
        algorithmList.put(3, "redistribution_binary_merge");
        algorithmList.put(4, "redistribution_merge_all");
        algorithmList.put(5, "partitioned");
        return algorithmList;
    }

    @PostMapping
    public Response parallelBinaryMerge(@RequestBody Request request) throws IOException, JSONException {
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

        StringBuilder result = new StringBuilder();
        result.append("[");
        int c = 1;
        for (Cpu<Integer> cpu : cpus) {
            result.append("{");
            result.append("label: '");
            result.append(cpu.getCpuName());
            result.append("',");

            result.append(").alpha(0.5).rgbString(),");

            result.append(",");

            result.append("borderWidth : 1,");

            result.append("data : [ ");
            switch (selectedAlgorithm) {
                case "merge_all":
                    result.append(getDataChartMergeAll(cpu));
                    break;
                case "binary_merge":
                    result.append(getDataChartBinaryMerge(cpu));
                    break;
                case "redistribution_binary_merge":
                    result.append(getDataRedistributionBinaryMerge(cpu));
                    break;
                case "redistribution_merge_all":
                    result.append(getDataRedistributionMergeAll(cpu));
                    break;
                case "partitioned":
                    result.append(getDataPartitionedSort(cpu));
                    break;
            }

            result.append("] ");
            result.append("},");
            c++;
        }

        response.setSuccessful(Boolean.TRUE);
        response.setResponse(new JSONObject(result.toString()));

        return response;
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
}
