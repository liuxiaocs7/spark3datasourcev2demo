package com.liuxiaocs.spark.sources.datasourcev2.simple

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}

class SimpleScanBuilder extends ScanBuilder {

  override def build(): Scan = new SimpleScan
}
