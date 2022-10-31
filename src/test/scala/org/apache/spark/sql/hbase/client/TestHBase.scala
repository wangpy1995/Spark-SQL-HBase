package org.apache.spark.sql.hbase.client

import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptorBuilder, Connection, Put, Scan, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{NamespaceDescriptor, TableName}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hbase.TConstants
import org.scalatest.funsuite.AnyFunSuite

/**
 * Created by wpy on 17-5-18.
 */
class TestHBase extends AnyFunSuite with Logging {

  private lazy val table = TConstants.TEST_NAMESPACE + ":" + TConstants.TEST_TABLE_NAME

  def dbExists(admin: Admin, dbName: String): Boolean = {
    admin.listNamespaceDescriptors.exists(_.getName == dbName)
  }

  def tableExists(admin: Admin, table: String): Boolean = {
    admin.tableExists(TableName.valueOf(table))
  }

  def createUserNamespaceAndTable(admin: Admin, namespaceName: String,
                                  table: String, cols: String*): Unit = {
    if (!admin.listNamespaces().contains(namespaceName)) {
      val namespace = NamespaceDescriptor.create(namespaceName).build()
      admin.createNamespace(namespace)
    }
    if (!admin.tableExists(TableName.valueOf(table))) {
      val tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(table))
      cols.foreach { colName =>
        val col = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colName)).build()
        tableDesc.setColumnFamily(col)
      }
      admin.createTable(tableDesc.build())
      admin.close()
      logInfo(s"Succeed in creating table $table")
    }
  }

  def insertData(conn: Connection, tableName: String,
                 maxRowCnt: Int, maxQualifierCnt: Int,
                 splitChar: String, columnNames: String*): Unit = {
    val bytesColumnNames = columnNames.map(Bytes.toBytes)

    //前面凑0对齐
    val rowPattern = s"%0${math.log10(maxRowCnt).toInt + 1}d"
    logInfo(s"Format Row with: $rowPattern")
    val qualifierPattern = s"%0${math.log10(maxQualifierCnt).toInt + 1}d"
    logInfo(s"Format qualifier with: $qualifierPattern")
    val splitterBytes = Bytes.toBytes(splitChar)
    val tableConn = conn.getTable(TableName.valueOf(tableName))

    def insertData(put: Put, bytesRowKey: Array[Byte], bytesColumnName: Array[Byte],
                   formattedQualifier: String, splitterBytes: Array[Byte]) = {
      //pattern: col+${splitter}+qualifier, A_01
      val qualifierBytes = Bytes.add(bytesColumnName, splitterBytes, Bytes.toBytes(formattedQualifier))
      //pattern: col+${splitter}+qualifier+${splitter}+rowKey, A_01_0001
      val value = Bytes.add(qualifierBytes, splitterBytes, bytesRowKey)
      put.addColumn(bytesColumnName, qualifierBytes, value)
      put
    }

    for (i <- 0 until TConstants.MAX_ROW_CNT) {
      val rowKey = i.toString.format(rowPattern)
      val bytesRowKey = Bytes.toBytes(rowKey)
      val put = new Put(Bytes.toBytes(rowKey))

      for (j <- 0 until TConstants.MAX_QUALIFIER_CNT) {
        val formattedQualifier = j.toString.format(qualifierPattern)
        //col value pattern: qualifier_rowKey
        bytesColumnNames.foreach { bytesColumnName =>
          insertData(put, bytesRowKey, bytesColumnName, formattedQualifier, splitterBytes)
        }
        tableConn.put(put)
      }
    }
    logInfo(s"Succeed in inserting ${TConstants.MAX_ROW_CNT} rows")
    tableConn.close()
    conn.close()
  }

  def scan(conn: Connection, tableName: String, cols: String*): Unit = {

    import scala.collection.JavaConverters._

    val scan = new Scan()
    //    scan.withStartRow(Bytes.toBytes("")).withStopRow(Bytes.toBytes(""))
    scan.addFamily(Bytes.toBytes(TConstants.TEST_COL_A))
    scan.addFamily(Bytes.toBytes(TConstants.TEST_COL_B))
    val cnt = conn.getTable(TableName.valueOf(tableName)).getScanner(scan).iterator().asScala.count(_ => true)
    logInfo(s"max row numbers: $cnt")
    conn.close()
  }

  def dropTable(admin: Admin, table: String): Unit = {
    val tableName = TableName.valueOf(table)
    while (admin.tableExists(tableName)) {
      while (!admin.isTableDisabled(tableName)) {
        admin.disableTable(tableName)
      }
      admin.deleteTable(tableName)
    }
    logInfo(s"Succeed in dropping table: $table")
    admin.close()
  }


  test("create namespace and table") {
    createUserNamespaceAndTable(
      TConstants.admin,
      TConstants.TEST_NAMESPACE,
      table,
      TConstants.TEST_COL_A,
      TConstants.TEST_COL_B)
  }

  test("insert some data") {
    insertData(TConstants.conn, table,
      1000,
      10,
      "_",
      TConstants.TEST_COL_A,
      TConstants.TEST_COL_B)
  }

  test("scan") {
    scan(TConstants.conn,
      table,
      TConstants.TEST_COL_A,
      TConstants.TEST_COL_B)
  }

  test("delete") {
    dropTable(TConstants.admin, table)
  }


}

object TestHBase {
  val testHBase = new TestHBase()

  def main(args: Array[String]): Unit = {
    testHBase.createUserNamespaceAndTable(TConstants.admin, TConstants.TEST_NAMESPACE, testHBase.table)
    testHBase.insertData(TConstants.conn, testHBase.table,
      1000, 10, "_",
      TConstants.TEST_COL_A,
      TConstants.TEST_COL_B)
    testHBase.scan(TConstants.conn, testHBase.table, TConstants.TEST_COL_A, TConstants.TEST_COL_B)
  }
}
