package com.distributedDatabase.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;


@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProcessResponse {
    private String process;
    private DataProcessResponse datas;
}
