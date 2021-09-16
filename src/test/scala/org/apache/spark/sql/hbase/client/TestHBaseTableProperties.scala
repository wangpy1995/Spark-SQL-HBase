package org.apache.spark.sql.hbase.client

import org.apache.spark.sql.hbase.TConstants._
import org.scalatest.funsuite.AnyFunSuite

/**
 * Created by wpy on 17-5-13.
 */
class TestHBaseTableProperties extends AnyFunSuite {
  def testPropertiesAsString(dbName: String, tableName: String): String = {
    val cli = IsolatedClientLoader.forVersion("3.0.0", "3.2.0", sparkConf, conf, extraConfig).createClient()
    cli.getTableOption(dbName, tableName).get.properties.mkString("\n")
  }

  def testExecutor(): Unit = {
    val pattern = "\\{([0-9a-zA-Z]*):\\((([0-9a-zA-Z]*([,])?)*)\\)}".r
    val str = "{CF1:(Q1, Q2, Q3, Qn)}".replaceAll("\t", " ").replaceAll(" ", "")
    /* val p = pattern.pattern matcher str
     var x = List.empty[String]
     if (p.matches())
       x :::=
         Some((1 to p.groupCount).toList map p.group).get
     else None
     x*/
    str match {
      case pattern(columnFamily, qualifiers, _*) => qualifiers.split(",").foreach(q => println(columnFamily + ":" + q))
      case _ => println("none")
    }
  }

  def testReduce(): Unit = {
    val bloom = "CF1: type; CF2: type; CF3: type; CFn: type".replaceAll("\t", "").replaceAll(" ", "")
    val r = bloom.split(";").map { b =>
      val r = b.split(":")
      Map(r(0) -> r(1))
    }.reduce(_ ++ _)
    println(r)
  }

  test("print table properties") {
    println(testPropertiesAsString(TEST_NAMESPACE, TEST_TABLE_NAME))
  }

  test("reduce string") {
    testReduce()
  }
}
