package com.fcbai.parquet.sdk;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class CustomSimpleRecordMaterializer extends RecordMaterializer<CustomSimpleRecord> {
    public final CustomSimpleRecordConverter root;

    public CustomSimpleRecordMaterializer(MessageType schema) {
        this.root = new CustomSimpleRecordConverter(schema);
    }

    @Override
    public CustomSimpleRecord getCurrentRecord() {
        return root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }
}
