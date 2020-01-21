package com.distributedDatabase.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataProcessResponse {
    private String algorithm;
    private String title;
    private String data;
}
