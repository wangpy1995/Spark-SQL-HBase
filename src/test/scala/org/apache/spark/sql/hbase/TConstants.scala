package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by wpy on 17-5-13.
 */
private[hbase] object TConstants {
  private[hbase] lazy val conf = new Configuration()
  private[hbase] lazy val hConf = HBaseConfiguration.create(conf)
  private[hbase] lazy val conn = ConnectionFactory.createConnection(hConf)
  private[hbase] lazy val admin = conn.getAdmin

  private[hbase] lazy val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("test")
    .set("spark.hadoopRDD.ignoreEmptySplits", "false")
    .set("spark.driver.port", "4042")

  private[hbase] lazy val ss = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  private[hbase] lazy val sc = ss.sparkContext
  private[hbase] lazy val extraConfig = Map(
    "schema.file.url" -> "/home/pw/IdeaProjects/Spark-SQL-HBase/src/main/resources/test.yml",
    "spark.hbase.client.impl" -> "org.apache.spark.sql.hbase.client.HBaseClientImpl")
  private[hbase] lazy val hs = new HBaseSession(
    TConstants.sc,
    conf,
    extraConfig)

  //namespace and table's name
  private[hbase] lazy val TEST_NAMESPACE = "pw"
  private[hbase] lazy val TEST_TABLE_NAME = "test"
  //column family
  private[hbase] lazy val TEST_COL_A = "A"
  private[hbase] lazy val TEST_COL_B = "B"
  //max count of rows
  private[hbase] lazy val MAX_ROW_CNT = 1000
  //max count of qualifiers
  private[hbase] lazy val MAX_QUALIFIER_CNT = 10

}
