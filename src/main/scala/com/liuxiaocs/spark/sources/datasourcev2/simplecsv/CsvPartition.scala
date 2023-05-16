package com.liuxiaocs.spark.sources.datasourcev2.simplecsv

import org.apache.spark.sql.connector.read.InputPartition

case class CsvPartition(partitionNumber: Int, path: String, header: Boolean = true) extends InputPartition
