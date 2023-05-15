package com.liuxiaocs.spark.sources.datasourcev2.simplemulti

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class SimpleScan extends Scan with Batch {

  override def readSchema(): StructType = StructType(Array(StructField("value", StringType)))

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // multi partitions
    Array(new SimplePartition(0, 4), new SimplePartition(5, 9))
  }

  override def createReaderFactory(): PartitionReaderFactory = new SimplePartitionReaderFactory
}
