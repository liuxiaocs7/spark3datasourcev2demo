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

## CSV Source

```shell
root
 |-- OrganisationLabel: string (nullable = true)
 |-- OrganisationURI: string (nullable = true)
 |-- PublishedDate: string (nullable = true)
 |-- DurationFrom: string (nullable = true)
 |-- DurationTo: string (nullable = true)
 |-- LatestData: string (nullable = true)
 |-- ReportingPeriodType: string (nullable = true)
 |-- GeoEntityName: string (nullable = true)
 |-- GeoCode: string (nullable = true)
 |-- GeoName: string (nullable = true)
 |-- GeoURI: string (nullable = true)
 |-- Year: string (nullable = true)
 |-- CO2EmissionsTonnesPerPerson: string (nullable = true)


+-----------------+--------------------+-------------------+-------------------+-------------------+----------+-------------------+----------------+---------+--------------+--------------------+----+---------------------------+
|OrganisationLabel|     OrganisationURI|      PublishedDate|       DurationFrom|         DurationTo|LatestData|ReportingPeriodType|   GeoEntityName|  GeoCode|       GeoName|              GeoURI|Year|CO2EmissionsTonnesPerPerson|
+-----------------+--------------------+-------------------+-------------------+-------------------+----------+-------------------+----------------+---------+--------------+--------------------+----+---------------------------+
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2020-01-01T00:00:00|2020-12-31T23:59:59|      TRUE|      Calendar Year|          County|E10000019|  Lincolnshire|http://statistics...|2020|                        4.1|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2020-01-01T00:00:00|2020-12-31T23:59:59|      TRUE|      Calendar Year|District Council|E07000136|        Boston|http://statistics...|2020|                        3.7|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2020-01-01T00:00:00|2020-12-31T23:59:59|      TRUE|      Calendar Year|District Council|E07000137|  East Lindsey|http://statistics...|2020|                        4.3|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2020-01-01T00:00:00|2020-12-31T23:59:59|      TRUE|      Calendar Year|District Council|E07000138|       Lincoln|http://statistics...|2020|                        2.8|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2020-01-01T00:00:00|2020-12-31T23:59:59|      TRUE|      Calendar Year|District Council|E07000139|North Kesteven|http://statistics...|2020|                        4.2|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2020-01-01T00:00:00|2020-12-31T23:59:59|      TRUE|      Calendar Year|District Council|E07000140| South Holland|http://statistics...|2020|                        4.5|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2020-01-01T00:00:00|2020-12-31T23:59:59|      TRUE|      Calendar Year|District Council|E07000141|South Kesteven|http://statistics...|2020|                        4.6|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2020-01-01T00:00:00|2020-12-31T23:59:59|      TRUE|      Calendar Year|District Council|E07000142|  West Lindsey|http://statistics...|2020|                        4.6|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2019-01-01T00:00:00|2019-12-31T23:59:59|          |      Calendar Year|          County|E10000019|  Lincolnshire|http://statistics...|2019|                        4.7|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2019-01-01T00:00:00|2019-12-31T23:59:59|          |      Calendar Year|District Council|E07000136|        Boston|http://statistics...|2019|                        4.2|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2019-01-01T00:00:00|2019-12-31T23:59:59|          |      Calendar Year|District Council|E07000137|  East Lindsey|http://statistics...|2019|                          5|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2019-01-01T00:00:00|2019-12-31T23:59:59|          |      Calendar Year|District Council|E07000138|       Lincoln|http://statistics...|2019|                        3.3|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2019-01-01T00:00:00|2019-12-31T23:59:59|          |      Calendar Year|District Council|E07000139|North Kesteven|http://statistics...|2019|                        4.6|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2019-01-01T00:00:00|2019-12-31T23:59:59|          |      Calendar Year|District Council|E07000140| South Holland|http://statistics...|2019|                        4.9|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2019-01-01T00:00:00|2019-12-31T23:59:59|          |      Calendar Year|District Council|E07000141|South Kesteven|http://statistics...|2019|                        5.3|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2019-01-01T00:00:00|2019-12-31T23:59:59|          |      Calendar Year|District Council|E07000142|  West Lindsey|http://statistics...|2019|                        5.1|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2018-01-01T00:00:00|2018-12-31T23:59:59|          |      Calendar Year|          County|E10000019|  Lincolnshire|http://statistics...|2018|                          5|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2018-01-01T00:00:00|2018-12-31T23:59:59|          |      Calendar Year|District Council|E07000136|        Boston|http://statistics...|2018|                        4.5|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2018-01-01T00:00:00|2018-12-31T23:59:59|          |      Calendar Year|District Council|E07000137|  East Lindsey|http://statistics...|2018|                        5.2|
|     Lincolnshire|http://opendataco...|2022-07-01T00:00:00|2018-01-01T00:00:00|2018-12-31T23:59:59|          |      Calendar Year|District Council|E07000138|       Lincoln|http://statistics...|2018|                        3.5|
+-----------------+--------------------+-------------------+-------------------+-------------------+----------+-------------------+----------------+---------+--------------+--------------------+----+---------------------------+
only showing top 20 rows
```