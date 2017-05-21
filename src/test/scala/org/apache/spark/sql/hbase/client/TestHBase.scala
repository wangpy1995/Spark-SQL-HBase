package org.apache.spark.sql.hbase.client

import org.apache.hadoop.hbase.TableName
import org.apache.spark.sql.hbase.TConstants

/**
  * Created by wpy on 17-5-18.
  */
object TestHBase {
  private val admin = TConstants.admin
  private val conn = TConstants.conn

  def dbExists(dbName: String): Boolean = {
    admin.listNamespaceDescriptors.exists(_.getName == dbName)
  }

  def tableExists(table: String): Boolean = {
    admin.tableExists(TableName.valueOf(table))
  }

  def main(args: Array[String]): Unit = {
    //    println(dbExists("global_temp"))
    //    val tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf("wpy1:test"))
    //    tableDesc.addFamily(new HColumnDescriptor("index")).addFamily(new HColumnDescriptor("cf")).addFamily(new HColumnDescriptor("user"))
    //    admin.createTable(tableDesc.build())
    /*
        for (i <- 0 until 1000) {
          val rowKey = i.formatted("%04d").toString
          val put = new Put(Bytes.toBytes(rowKey))
          for (j <- 0 until 10) {
            put.addColumn(Bytes.toBytes("index"), Bytes.toBytes(s"idx_$j"), Bytes.toBytes(rowKey + "index"))
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(s"cf_$j"), Bytes.toBytes(rowKey + "column_family"))
            put.addColumn(Bytes.toBytes("user"), Bytes.toBytes(s"usr_$j"), Bytes.toBytes(rowKey + "user"))
            conn.getTable(TableName.valueOf("wpy1:test")).put(put)
          }
        }
        conn.close()*/
    val family = Array("index", "cf", "user")
    //    val col :mutable.Buffer[(String,String)]= ("index", "idx_1")  ("index", "idx_2") += ("index", "idx_3") += ("index", "idx_4") += ("cf", "column_family_1") += ("cf", "column_family_2") += ("cf", "column_family_3") += ("usr", "user_1") += ("usr", "user_2") += ("usr", "user_3")
    "cf_cf_1".split("_", 2).foreach(println)
  }

}
