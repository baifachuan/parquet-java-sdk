package com.fcbai.parquet.sdk;

import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.filter.PagedRecordFilter.page;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;

public class CustomParquetWriterTest {

    @Test
    public void test_write_to_parquet_by_java() throws IOException {
        List<User> users = new ArrayList<>();
        User user1 = new User("1","fcbai","123123");
        User user2 = new User("2","fcbai","123445");
        users.add(user1);
        users.add(user2);
        Path dataFile = new Path("./src/test/resources/demo.snappy.parquet");
        // Write as Parquet file.
        try (ParquetWriter<User> writer = AvroParquetWriter.<User>builder(dataFile)
                .withSchema(ReflectData.AllowNull.get().getSchema(User.class))
                .withDataModel(ReflectData.get())
                .withConf(new Configuration())
                .withCompressionCodec(SNAPPY)
                .withWriteMode(OVERWRITE)
                .build()) {
            for (User user : users) {
                writer.write(user);
            }
        }
    }

    @Test
    public void test_read_partition_file_by_pre_write() throws IOException {
        ParquetReader<CustomSimpleRecord> reader = ParquetReader
                .builder(new CustomSimpleReadSupport(), new Path("./src/test/resources/demo.snappy.parquet"))
                .withFilter(FilterCompat.get(page(1, 10)))
                .build();
        for (CustomSimpleRecord value = reader.read(); value != null; value = reader.read()) {
            System.out.println(value.toJson());
        }
        reader.close();
    }

}
