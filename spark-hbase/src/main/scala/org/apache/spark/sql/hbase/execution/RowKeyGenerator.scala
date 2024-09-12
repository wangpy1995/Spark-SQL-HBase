package org.apache.spark.sql.hbase.execution

import org.apache.spark.sql.Row

//用于插入HBase时生成rowKey
trait RowKeyGenerator extends Serializable {

  def genRowKey(row: Row): Array[Byte]

}