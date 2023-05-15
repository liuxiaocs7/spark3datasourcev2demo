package com.liuxiaocs.spark.sources.datasourcev2.simple

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String

class SimplePartitionReader extends PartitionReader[InternalRow] {

  private val values: Array[String] = Array("1", "2", "3", "4", "5", "6", "7")

  private var idx = 0

  override def next(): Boolean = idx < values.length

  override def get(): InternalRow = {
    val stringValue = values(idx)
    val stringUtf = UTF8String.fromString(stringValue)
    val row = InternalRow(stringUtf)
    idx = idx + 1
    row
  }

  override def close(): Unit = Unit
}
