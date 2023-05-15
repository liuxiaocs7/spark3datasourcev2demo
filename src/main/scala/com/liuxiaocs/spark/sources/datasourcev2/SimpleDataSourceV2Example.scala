package com.liuxiaocs.spark.sources.datasourcev2

import org.apache.spark.sql.SparkSession

object SimpleDataSourceV2Example {
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

  }
}
