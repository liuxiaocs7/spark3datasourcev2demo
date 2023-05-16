package com.liuxiaocs.spark.sources.datasourcev2.simplecsv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String

class CsvPartitionReader(inputPartition: CsvPartition) extends PartitionReader[InternalRow] {

  private var iterator: Iterator[String] = null

  @transient
  override def next(): Boolean = {
    if (iterator == null) {
      val sparkContext = SparkSession.builder().getOrCreate().sparkContext
      val rdd = sparkContext.textFile(inputPartition.path)
      val filterRDD = if (inputPartition.header) {
        val firstLine = rdd.first
        rdd.filter(_ != firstLine)
      } else rdd
      val partition = filterRDD.partitions(inputPartition.partitionNumber)
      iterator = filterRDD.iterator(partition, org.apache.spark.TaskContext.get())
    }
    iterator.hasNext
  }

  override def get(): InternalRow = {
    val line = iterator.next()
    InternalRow.fromSeq(line.split(",").map(value => UTF8String.fromString(value)))
  }

  override def close(): Unit = Unit
}
