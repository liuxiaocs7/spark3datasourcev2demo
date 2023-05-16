package com.liuxiaocs.spark.sources.datasourcev2.simplemysql

import org.apache.spark.sql.connector.write.{BatchWrite, WriteBuilder}

class MysqlWriterBuilder extends WriteBuilder {

  override def buildForBatch(): BatchWrite = new MysqlBatchWriter
}
