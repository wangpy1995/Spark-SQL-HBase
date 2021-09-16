package org.apache.spark.sql.hbase.client

import org.apache.hadoop.hbase.{NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, Put, Scan, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hbase.TConstants
import org.scalatest.funsuite.AnyFunSuite

/**
 * Created by wpy on 17-5-18.
 */
class TestHBase extends AnyFunSuite {
  private val admin = TConstants.admin
  private val conn = TConstants.conn

  def dbExists(dbName: String): Boolean = {
    admin.listNamespaceDescriptors.exists(_.getName == dbName)
  }

  def tableExists(table: String): Boolean = {
    admin.tableExists(TableName.valueOf(table))
  }

  def createUserNamespaceAndTable(): Unit = {
    val namespaceName = "wpy1"
    val tableName = "wpy1:test"
    if (!admin.listNamespaces().contains(namespaceName)) {
      val namespace = NamespaceDescriptor.create(namespaceName).build()
      admin.createNamespace(namespace)
    }
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      val tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf("wpy1:test"))
      val cf1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf1")).build()
      val cf2 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf2")).build()
      tableDesc.setColumnFamily(cf1)
      tableDesc.setColumnFamily(cf2)
      admin.createTable(tableDesc.build())
      conn.close()
    }
  }

  def insertData(): Unit = {
    for (i <- 0 until 1000) {
      val rowKey = i.formatted("%04d").toString
      val put = new Put(Bytes.toBytes(rowKey))
      for (j <- 0 until 10) {

        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(s"cf1_$j"),
          Bytes.toBytes(i + "column_family1_" + "_" + j + "_" + rowKey))

        put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes(s"cf2_$j"),
          Bytes.toBytes(i + "column_family2_" + "_" + j + "_" + rowKey))

        conn.getTable(TableName.valueOf("wpy1:test")).put(put)
      }
    }
    conn.close()
  }

  def scan(): Unit = {
    import scala.collection.JavaConverters._
    val scan = new Scan()
    //    scan.withStartRow(Bytes.toBytes("")).withStopRow(Bytes.toBytes(""))
    scan.addFamily(Bytes.toBytes("cf1"))
    scan.addFamily(Bytes.toBytes("cf2"))
    val cnt = conn.getTable(TableName.valueOf("wpy1:test")).getScanner(scan).iterator().asScala.count(_ => true)
    println(cnt)
    conn.close()
  }


  test("create namespace and table") {
    createUserNamespaceAndTable()
  }

  test("insert some data") {
    insertData()
  }

  test("scan") {
    scan()
  }


}

object TestHBase {
  val testHBase = new TestHBase()

  def main(args: Array[String]): Unit = {
    testHBase.createUserNamespaceAndTable()
    testHBase.insertData()
    testHBase.scan()
  }
}
