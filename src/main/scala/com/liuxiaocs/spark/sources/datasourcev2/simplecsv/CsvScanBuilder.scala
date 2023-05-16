package com.liuxiaocs.spark.sources.datasourcev2.simplecsv

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}

class CsvScanBuilder(path: String) extends ScanBuilder {

  override def build(): Scan = new CsvScan(path)
}
