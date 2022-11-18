package org.apache.spark.sql.hbase.execution

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.io.hfile.HFile
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hbase.TConstants._
import org.scalatest.funsuite.AnyFunSuite

class HBaseFileFormatTestSuite extends AnyFunSuite {
  val hadoopConf = new Configuration()
  val hfilePath = "/home/pw/IdeaProjects/hbase/tmp/hbase/data/pw/test/2debdc5e3e71d4e9693a4f10ac74b521/A/a99e95dd0637431cbe07cde90adc28b1"

  test("read hfile") {
    var lastKey: String = null
    val filePath = new Path(hfilePath)
    val fs = FileSystem.get(hadoopConf)
    val hfileReader = HFile.createReader(fs, filePath, hadoopConf)

    val scanner = hfileReader.getScanner(hConf, false, false)
    scanner.seekTo()
    0 until 10 foreach { _ =>
      if (scanner.next()) {
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
  }

  test("read hfile by sql") {
    ss.sql(
      s"""
         |CREATE TABLE test(
         |    row_key string,
         |    A_00 string,
         |    A_01 string,
         |    A_02 string,
         |    A_03 string,
         |    A_04 string,
         |    A_05 string,
         |    A_06 string,
         |    A_07 string,
         |    A_08 string,
         |    A_09 string
         |) USING ${classOf[HBaseFileFormat].getCanonicalName}
         |OPTIONS(
         |path '$hfilePath')
         |""".stripMargin).show()
    ss.sql("select * from test").show()
    ss.sql("select A_00 from test").show()
    ss.sql("select row_key from test").show()
    assert(ss.sql("select * from test").count() == 1000)
  }
}
