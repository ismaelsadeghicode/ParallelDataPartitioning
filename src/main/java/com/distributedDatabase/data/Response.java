package com.distributedDatabase.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Response<T> {
    private boolean successful;
    private String algorithm;
    private String errorDescription;
    private T response;
    private List<Integer> storageNO;
    private List<Integer> storageSortNO;
}
