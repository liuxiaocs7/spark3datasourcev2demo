package com.liuxiaocs.spark.sources.datasourcev2.simplecsv

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters.setAsJavaSetConverter

class CsvBatchTable(path: String) extends Table with SupportsRead {

  override def name(): String = this.getClass.toString

  override def schema(): StructType = SchemaUtils.getSchema(path)

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new CsvScanBuilder(path)
}
