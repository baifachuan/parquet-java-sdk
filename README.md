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

more info and cn version to see: https://baifachuan.com/posts/2e6dc139.html