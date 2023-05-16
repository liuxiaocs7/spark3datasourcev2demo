package com.liuxiaocs.spark.sources.datasourcev2.simplecsv

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class DefaultSource extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    getTable(null, Array.empty[Transform], options.asCaseSensitiveMap()).schema()


  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    val path = properties.get("path")
    new CsvBatchTable(path)
  }
}
