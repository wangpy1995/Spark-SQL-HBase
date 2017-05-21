package org.apache.spark.sql.hbase

/**
  * Created by wpy on 17-5-17.
  */
object TestSql {

  def testSql(): Unit = {
  }

  def main(args: Array[String]): Unit = {
    /*
        val hc = new HBaseSession(TConstants.sc,new Configuration())
    //    hc.sql("create database wpy")
    //    hc.sql("create table wpy.test")
        val query = hc.sql("select * from wpy.test")
        val execution = query.queryExecution
        execution.analyzed
        query.show()
    */
    "123,45,6".split(",").filter(_.matches(".*")).foreach(println)

  }
}
