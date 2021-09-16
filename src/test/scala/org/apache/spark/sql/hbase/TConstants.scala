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

  private[hbase] lazy val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.hadoopRDD.ignoreEmptySplits","false").set("spark.driver.port","4042")
  private[hbase] lazy val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  private[hbase] lazy val sc = ss.sparkContext
  private[hbase] lazy val extraConfig = Map(
    "schema.file.url" -> "/home/wpy/IdeaProjects/Spark-SQL-HBase/src/main/resources/test.yml",
    "spark.hbase.client.impl" -> "org.apache.spark.sql.hbase.client.HBaseClientImpl")
  private[hbase] lazy val hs = new HBaseSession(
    TConstants.sc,
    conf,
    extraConfig)

  private[hbase] lazy val TEST_TABLE_NAME = "test"
  private[hbase] lazy val TEST_NAMESPACE = "wpy1"

}
