package com.liuxiaocs.spark.sources.datasourcev2.simplemulti

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String

class SimplePartitionReader(inputPartition: SimplePartition) extends PartitionReader[InternalRow] {

  private val values: Array[String] = Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")

  private var idx = inputPartition.start

  override def next(): Boolean = idx < inputPartition.end

  override def get(): InternalRow = {
    val stringValue = values(idx)
    val stringUtf = UTF8String.fromString(stringValue)
    val row = InternalRow(stringUtf)
    idx = idx + 1
    row
  }

  override def close(): Unit = Unit
}
