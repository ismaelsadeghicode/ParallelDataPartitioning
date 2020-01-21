package com.distributedDatabase.controller;

import com.distributedDatabase.data.Request;
import com.distributedDatabase.data.Response;
import com.distributedDatabase.services.ParallerSortService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

@RestController
@RequestMapping("parallel-sort")
public class ParallelSortController {

    @Autowired
    private ParallerSortService service;

    @GetMapping
    public Map<Integer, String> listOfAlgorithms() {
        return service.listOfAlgorithms();
    }

    @PostMapping
    public Response parallelBinaryMerge(@RequestBody Request request) throws IOException {
        return service.parallelBinaryMerge(request);
    }
}
