package com.liuxiaocs.spark.sources.datasourcev2.simplemulti

import org.apache.spark.sql.connector.read.InputPartition

class SimplePartition(val start: Int, val end: Int) extends InputPartition
