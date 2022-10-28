package org.apache.spark.sql.hbase.execution

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.io.hfile.HFile
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hbase.TConstants._
import org.scalatest.funsuite.AnyFunSuite

class HBaseFileFormatTestSuite extends AnyFunSuite {
  val hadoopConf = new Configuration()
  val hfilePath = "/home/pw/tmp/hbase/data/wpy1/test/475684a4f75db4114dbb4b0ff1ac6a01/cf2/7344cc44459e47bfbc1ce6b68a9c8bd8"

  test("read hfile") {
    var lastKey: String = null
    val filePath = new Path(hfilePath)
    val fs = FileSystem.get(hadoopConf)
    val hfileReader = HFile.createReader(fs, filePath, hadoopConf)

    val scanner = hfileReader.getScanner(hConf,false, false)
    scanner.seekTo()
    while (scanner.next()) {
      val cell = scanner.getCell
      val curKey = Bytes.toString(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
      if (null != curKey && lastKey != curKey) {
        println("====================================================================")
        println(s"key: $curKey")
        lastKey = curKey
      }
      println(s"column family: ${Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)}")
      println(s"qualifier: ${Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)}")
      println(s"value: ${Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)}")
    }
  }

  test("read hfile by sql") {
    ss.sql(
      s"""
         |CREATE TABLE test(
         |    row_key string,
         |    cf2_0 string,
         |    cf2_1 string,
         |    cf2_2 string,
         |    cf2_3 string,
         |    cf2_4 string,
         |    cf2_5 string,
         |    cf2_6 string,
         |    cf2_7 string,
         |    cf2_8 string,
         |    cf2_9 string
         |) USING ${classOf[HBaseFileFormat].getTypeName}
         |OPTIONS(
         |path '$hfilePath')
         |""".stripMargin).show()
    ss.sql("select * from test").show()
    ss.sql("select cf2_0 from test").show()
    ss.sql("select row_key from test").show()
    assert(ss.sql("select * from test").count() == 1000)
  }
}
