package com.fcbai.parquet.sdk.reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.parquet.tools.read.SimpleRecord;

import java.util.Map;

public class CustomSimpleRecord extends SimpleRecord {
    public String toJson() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(this.toJsonObject());
    }

    protected Object toJsonObject() {
        Map<String, Object> result = Maps.newLinkedHashMap();
        for (NameValue value : values) {
            result.put(value.getName(), toJsonValue(value.getValue()));
        }
        return result;
    }
}
