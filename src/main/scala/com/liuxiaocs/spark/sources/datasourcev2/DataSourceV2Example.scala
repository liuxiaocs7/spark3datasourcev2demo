package com.liuxiaocs.spark.sources.datasourcev2

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSourceV2Example {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config("spark.ui.port", "9091")
      .getOrCreate()

    val simpleDF = sparkSession
      .read.format("com.liuxiaocs.spark.sources.datasourcev2.simple")
      .load()
    simpleDF.show()

    println(s"number of partitions in simple source is ${simpleDF.rdd.getNumPartitions}")

    val simpleMultiDF = sparkSession
      .read.format("com.liuxiaocs.spark.sources.datasourcev2.simplemulti")
      .load()

    simpleMultiDF.show()
    println(s"number of partitions in simple multi source is ${simpleMultiDF.rdd.getNumPartitions}")

    val simpleCsvDF = sparkSession
      .read.format("com.liuxiaocs.spark.sources.datasourcev2.simplecsv")
      .load("src/main/resources/co2emissions.csv")

    simpleCsvDF.printSchema()
    simpleCsvDF.show()
    println(s"number of partitions in simple csv source is ${simpleCsvDF.rdd.getNumPartitions}")

    val simpleMysqlDF = sparkSession.createDataFrame(Seq(
      Tuple1("test1"),
      Tuple1("test2")
    )).toDF("user")

    // writer examples
    simpleMysqlDF.write
      .format("com.liuxiaocs.spark.sources.datasourcev2.simplemysql")
      .mode(SaveMode.Append)
      .save()

    sparkSession.stop()
  }
}
