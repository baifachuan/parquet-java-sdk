package com.fcbai.parquet.sdk.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class CustomSimpleReadSupport extends ReadSupport<CustomSimpleRecord> {
    @Override
    public RecordMaterializer<CustomSimpleRecord> prepareForRead(Configuration conf, Map<String,String> metaData, MessageType schema, ReadContext context) {
        return new CustomSimpleRecordMaterializer(schema);
    }

    @Override
    public ReadContext init(InitContext context) {
        return new ReadContext(context.getFileSchema());
    }
}
