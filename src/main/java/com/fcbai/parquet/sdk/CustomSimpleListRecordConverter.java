package com.fcbai.parquet.sdk;

import org.apache.parquet.schema.GroupType;

public class CustomSimpleListRecordConverter extends CustomSimpleRecordConverter {

    public CustomSimpleListRecordConverter(GroupType schema, String name, CustomSimpleRecordConverter parent) {
        super(schema, name, parent);
    }

    @Override
    public void start() {
        record = new CustomSimpleListRecord();
    }

}
