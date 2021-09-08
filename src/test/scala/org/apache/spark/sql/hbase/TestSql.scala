package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * Created by wpy on 17-5-17.
 */
object TestSql {

  def testSql(): Unit = {
  }

  def testNewHBaseRDD(hc: HBaseSession) = {
    val job: Job = Job.getInstance(hc.config)
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("cf1"))
    scan.addFamily(Bytes.toBytes("cf2"))

    TableMapReduceUtil.initCredentials(job)
    TableMapReduceUtil.initTableMapperJob(TableName.valueOf("wpy1:test"), scan,
      classOf[IdentityTableMapper], null, null, job)

    val jconf = new JobConf()
    SparkHadoopUtil.get.addCredentials(jconf)
    val rdd = hc.sparkContext.newAPIHadoopRDD(job.getConfiguration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val count = rdd.map { a =>
      println(a)
      a
    }.count()
    println(count)
  }

  def main(args: Array[String]): Unit = {
    val hc = new HBaseSession(
      TConstants.sc,
      new Configuration(),
      Map(
        "schema.file.url" -> "/home/wpy/IdeaProjects/Spark-SQL-HBase/src/main/resources/test.yml",
        "spark.hbase.client.impl" -> "org.apache.spark.sql.hbase.client.HBaseClientImpl"))
    val query = hc.sql("select * from hbase.meta")
    query.show()
    while (true) {
      Thread.sleep(10000)
    }
    //   "123,45,6".split(",").filter(_.matches(".*")).foreach(println)

  }
}
