# parquet-java-sdk
Simple SDK for parquet write by java

you can using the sdk to write a parquet file or read the parquet file and convert the data to standard json.

the parquet default json formart :

```json
data1: value1
data2: value2
models
  map
    key: data3
    value
      array: value3
  map
    key: data4
    value
      array: value4
data5: value5
```

but we need:
```json
"data1": "value1",
"data2": "value2",
"models": {
    "data3": [
        "value3"
    ],
    "data4": [
        "value4"
    ]
},
"data5": "value5"
...
```

the example for read in : com.fcbai.parquet.sdk.CustomParquetReaderTest
```java
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
```

the write example:

```java
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
```


more info and cn version to see: https://baifachuan.com/posts/2e6dc139.html
