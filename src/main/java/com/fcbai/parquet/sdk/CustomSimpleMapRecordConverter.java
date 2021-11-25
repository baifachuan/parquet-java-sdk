package com.fcbai.parquet.sdk.reader;

import org.apache.parquet.schema.GroupType;

public class CustomSimpleMapRecordConverter extends CustomSimpleRecordConverter {

    public CustomSimpleMapRecordConverter(GroupType schema, String name, CustomSimpleRecordConverter parent) {
        super(schema, name, parent);
    }

    @Override
    public void start() {
        record = new CustomSimpleMapRecord();
    }

}
