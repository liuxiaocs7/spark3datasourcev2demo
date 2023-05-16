package com.liuxiaocs.spark.sources.datasourcev2.simplecsv

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class CsvPartitionReaderFactory extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new CsvPartitionReader(partition.asInstanceOf[CsvPartition])
}
