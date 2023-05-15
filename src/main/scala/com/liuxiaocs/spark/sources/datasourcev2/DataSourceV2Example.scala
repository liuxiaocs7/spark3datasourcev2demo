package com.liuxiaocs.spark.sources.datasourcev2

import org.apache.spark.sql.SparkSession

object DataSourceV2Example {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
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

  }
}
