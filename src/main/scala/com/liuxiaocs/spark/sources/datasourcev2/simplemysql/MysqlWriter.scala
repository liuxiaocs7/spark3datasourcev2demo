package com.liuxiaocs.spark.sources.datasourcev2.simplemysql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}

import java.sql.DriverManager

class MysqlWriter extends DataWriter[InternalRow] {

  private val url = "jdbc:mysql://localhost/test"
  private val user = "root"
  private val password = "lx123456"
  private val table = "userwrite"

  private val connection = DriverManager.getConnection(url, user, password)
  private val statement = s"insert into ${table} (user) values (?)"
  private val preparedStatement = connection.prepareStatement(statement)

  override def write(record: InternalRow): Unit = {
    val value = record.getString(0)
    preparedStatement.setString(1, value)
    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = {}
}
