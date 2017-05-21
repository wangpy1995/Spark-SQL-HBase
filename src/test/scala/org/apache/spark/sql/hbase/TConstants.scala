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

  private[hbase] lazy val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
  private[hbase] lazy val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  private[hbase] lazy val sc = ss.sparkContext
}
