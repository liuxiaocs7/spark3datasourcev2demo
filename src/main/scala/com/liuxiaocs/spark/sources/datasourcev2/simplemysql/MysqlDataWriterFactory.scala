package com.liuxiaocs.spark.sources.datasourcev2.simplemysql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}

class MysqlDataWriterFactory extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = new MysqlWriter
}
