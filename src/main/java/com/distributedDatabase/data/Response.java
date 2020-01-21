package com.distributedDatabase.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Response<T> {
    private boolean successful;
    private String errorCode;
    private String errorDescription;
    private T response;
}
