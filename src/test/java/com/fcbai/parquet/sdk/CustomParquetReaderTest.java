package com.fcbai.parquet.sdk;


import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.filter.PagedRecordFilter.page;

public class CustomParquetReaderTest {

    String complexMapFilepath = "./src/test/resources/complex_map.parquet";
    String simpleFilepath = "./src/test/resources/simple.parquet";
    String mapFilePath = "./src/test/resources/map.parquet";
    String partitionFilePath = "./src/test/resources/partition.parquet";

    @Test
    public void test_read_complex_map_partition_table_content() throws IOException {
        ParquetReader<CustomSimpleRecord> reader = ParquetReader
                .builder(new CustomSimpleReadSupport(), new Path(complexMapFilepath))
                .withFilter(FilterCompat.get(page(1, 10)))
                .build();
        for (CustomSimpleRecord value = reader.read(); value != null; value = reader.read()) {
            System.out.println(value.toJson());
        }
        reader.close();
    }

    @Test
    public void test_read_simple_table_content() throws IOException {
        ParquetReader<CustomSimpleRecord> reader = ParquetReader
                .builder(new CustomSimpleReadSupport(), new Path(simpleFilepath))
                .withFilter(FilterCompat.get(page(1, 10)))
                .build();
        for (CustomSimpleRecord value = reader.read(); value != null; value = reader.read()) {
            System.out.println(value.toJson());
        }
        reader.close();
    }

    @Test
    public void test_read_map_table_content() throws IOException {
        ParquetReader<CustomSimpleRecord> reader = ParquetReader
                .builder(new CustomSimpleReadSupport(), new Path(mapFilePath))
                .withFilter(FilterCompat.get(page(1, 10)))
                .build();
        for (CustomSimpleRecord value = reader.read(); value != null; value = reader.read()) {
            System.out.println(value.toJson());
        }
        reader.close();
    }

    @Test
    public void test_read_partition_table_content() throws IOException {
        ParquetReader<CustomSimpleRecord> reader = ParquetReader
                .builder(new CustomSimpleReadSupport(), new Path(partitionFilePath))
                .withFilter(FilterCompat.get(page(1, 10)))
                .build();
        for (CustomSimpleRecord value = reader.read(); value != null; value = reader.read()) {
            System.out.println(value.toJson());
        }
        reader.close();
    }
}