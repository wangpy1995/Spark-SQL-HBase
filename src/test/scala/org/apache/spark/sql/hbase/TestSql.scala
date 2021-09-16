package org.apache.spark.sql.hbase

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter, SubstringComparator}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CompareOperator, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.hbase.TConstants._
import org.scalatest.funsuite.AnyFunSuite

/**
 * Created by wpy on 17-5-17.
 */
class TestSql extends AnyFunSuite {

  /**
   * 通过查询HBase数据来测试spark newAPIHadoopRDD是否正常
   *
   * @param hc
   */
  test("new newAPIHadoopRDD") {
    val job: Job = Job.getInstance(hs.config)
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("cf2_0"))
    scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("cf1_0"))
    val filter = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("cf1_0"), CompareOperator.EQUAL, new SubstringComparator("24"))
    filter.setFilterIfMissing(true)
    scan.setFilter(new FilterList(filter))

    TableMapReduceUtil.initCredentials(job)
    TableMapReduceUtil.initTableMapperJob(TableName.valueOf("wpy1:test"), scan,
      classOf[IdentityTableMapper], null, null, job)

    val jconf = new JobConf()
    SparkHadoopUtil.get.addCredentials(jconf)
    val rdd = hs.sparkContext.newAPIHadoopRDD(job.getConfiguration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val count = rdd.map { a =>
      println(a)
      a
    }.count()
    println(count)
  }

  test("select * ") {
    hs.sql("select * from hbase.meta").show()
    hs.sql(s"select * from $TEST_NAMESPACE.$TEST_TABLE_NAME")
  }
}
