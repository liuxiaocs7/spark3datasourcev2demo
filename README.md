# Spark3 DataSourceV2 Demo

## Simple DataSource

```shell
+-----+
|value|
+-----+
|    1|
|    2|
|    3|
|    4|
|    5|
|    6|
|    7|
+-----+
         
number of partitions in simple source is 1
```

## Multi Partitions DataSource

```shell
+-----+
|value|
+-----+
|    1|
|    2|
|    3|
|    4|
|    6|
|    7|
|    8|
|    9|
+-----+

number of partitions in simple multi source is 2
```