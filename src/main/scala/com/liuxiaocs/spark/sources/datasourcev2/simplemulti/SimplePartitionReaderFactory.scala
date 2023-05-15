package com.liuxiaocs.spark.sources.datasourcev2.simplemulti

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class SimplePartitionReaderFactory extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new SimplePartitionReader(partition.asInstanceOf[SimplePartition])
  }
}
