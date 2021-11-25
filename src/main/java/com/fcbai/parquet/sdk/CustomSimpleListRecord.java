package com.fcbai.parquet.sdk.reader;

import java.util.List;

public class CustomSimpleListRecord extends CustomSimpleRecord {
    @Override
    protected Object toJsonObject() {
        Object[] result = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            List<NameValue> data =  ((CustomSimpleRecord) values.get(i).getValue()).getValues();
            if (data.isEmpty()) {
                result[i] = toJsonValue(values.get(i).getValue());
            } else {
                result[i] = toJsonValue(data.get(0).getValue());
            }
        }
        return result;
    }
}

