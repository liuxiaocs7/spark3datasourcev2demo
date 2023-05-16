package com.liuxiaocs.spark.sources.datasourcev2.simplecsv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SchemaUtils {

  def getSchema(path: String): StructType = {
    val sparkContext = SparkSession.builder().getOrCreate().sparkContext
    val firstLine = sparkContext.textFile(path).first()
    val columnNames = firstLine.split(",")
    val structFields = columnNames.map(value => StructField(value, StringType))
    StructType(structFields)
  }
}
