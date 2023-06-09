package com.liuxiaocs.spark.sources.datasourcev2.simplecsv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

class CsvScan(path: String) extends Scan with Batch {

  override def readSchema(): StructType = SchemaUtils.getSchema(path)

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val sparkContext = SparkSession.builder().getOrCreate().sparkContext
    val rdd = sparkContext.textFile(path)
    val partitions = (0 to rdd.partitions.length - 1).map(value => CsvPartition(value, path))
    partitions.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = new CsvPartitionReaderFactory
}
