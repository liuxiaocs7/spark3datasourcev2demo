package com.liuxiaocs.spark.sources.datasourcev2.simple

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class SimpleScan extends Scan with Batch {

  override def readSchema(): StructType = StructType(Array(StructField("value", StringType)))

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // single partition
    Array(new SimplePartition())
  }

  override def createReaderFactory(): PartitionReaderFactory = new SimplePartitionReaderFactory
}
