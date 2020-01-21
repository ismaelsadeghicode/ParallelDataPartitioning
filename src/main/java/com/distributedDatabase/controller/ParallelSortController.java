package com.distributedDatabase.controller;

import com.distributedDatabase.data.Cpu;
import com.distributedDatabase.data.Request;
import com.distributedDatabase.data.Response;
import com.distributedDatabase.data.Storage;
import com.distributedDatabase.services.sort.*;
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
    public Response parallelBinaryMerge(@RequestBody Request request) throws IOException {
        cpuCount = request.getCpuCount();
        noRandomGeneration = request.getRandomNO();
        selectedAlgorithm = request.getAlgoritmNo();

        Response response = new Response();
        if (cpuCount % 2 != 0) {
            response.setSuccessful(Boolean.FALSE);
            response.setErrorDescription("The number of CPUs in a system is always even");
            return response;
        }
        if (cpuCount > noRandomGeneration) {
            response.setSuccessful(Boolean.FALSE);
            response.setErrorDescription("Random numbers are less than CPUs");
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
                response.setErrorDescription("The number of CPUs is not suitable for your algorithm of choice");
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
}
