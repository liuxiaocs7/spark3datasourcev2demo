package com.liuxiaocs.spark.sources.datasourcev2.simplemysql

import org.apache.spark.sql.connector.catalog.{SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{StringType, StructType}

import java.util
import scala.jdk.CollectionConverters.setAsJavaSetConverter

class MysqlTable extends SupportsWrite {

  private val tableSchema = new StructType().add("user", StringType)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new MysqlWriterBuilder

  override def name(): String = this.getClass.toString

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_WRITE, TableCapability.TRUNCATE).asJava
}
