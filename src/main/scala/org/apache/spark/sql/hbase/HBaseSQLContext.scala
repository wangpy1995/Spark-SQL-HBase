package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalogEvent, ExternalCatalogWithListener}
import org.apache.spark.sql.internal.{SessionState, SharedState, StaticSQLConf}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SerializableWritable, SparkContext}

import scala.annotation.meta.param
import scala.reflect.ClassTag

/**
 * Created by wpy on 17-5-16.
 */
class HBaseSQLContext private[hbase](@(transient@param) _hbaseSession: HBaseSession,
                                     @transient val config: Configuration,
                                     @transient extraConfig: Map[String, String],
                                     val tmpHdfsConfgFile: String = null)
  extends SQLContext(_hbaseSession) with Logging {
  self =>
  @transient private var tmpHdfsConfiguration: Configuration = config
  @transient private var appliedCredentials = false
  @transient private val job: Job = Job.getInstance(config)
  TableMapReduceUtil.initCredentials(job)
  @transient private var credentials = job.getCredentials
  private val broadcastedConf = _hbaseSession.sparkContext.broadcast(new SerializableWritable(config))
  private val credentialsConf = _hbaseSession.sparkContext.broadcast(new SerializableWritable(job.getCredentials))


  if (tmpHdfsConfgFile != null && config != null) {
    val fs = FileSystem.newInstance(config)
    val tmpPath = new Path(tmpHdfsConfgFile)
    if (!fs.exists(tmpPath)) {
      val outputStream = fs.create(tmpPath)
      config.write(outputStream)
      outputStream.close()
    } else {
      logWarning("tmpHdfsConfigDir " + tmpHdfsConfgFile + " exist!!")
    }
  }
  LatestHBaseContextCache.latest = this

  def this(sc: SparkContext, extraConfig: Map[String, String]) = {
    this(
      new HBaseSession(
        LatestHBaseContextCache.withHBaseExternalCatalog(sc),
        new Configuration(),
        extraConfig),
      new Configuration(),
      extraConfig)
  }

  def this(sc: JavaSparkContext, extraConfig: Map[String, String]) = this(sc.sc, extraConfig)

  /**
   * Returns a new HBaseContext as new session, which will have separated SQLConf, UDF/UDAF,
   * temporary tables and SessionState, but sharing the same CacheManager, IsolatedClientLoader
   * and HBase client (both of execution and metadata) with existing HBaseContext.
   */
  override def newSession(): HBaseSQLContext = {
    new HBaseSQLContext(_hbaseSession.newSession(), self.config, extraConfig)
  }

  /**
   * Invalidate and refresh all the cached the metadata of the given table. For performance reasons,
   * Spark SQL or the external data source library it uses might cache certain metadata about a
   * table, such as the location of blocks. When those change outside of Spark SQL, users should
   * call this function to invalidate the cache.
   *
   * @since 1.3.0
   */
  def refreshTable(tableName: String): Unit = {
    sparkSession.catalog.refreshTable(tableName)
  }


  def applyCreds[T]() {
    //    credentials = SparkHadoopUtil.get.getCurrentUserCredentials()
    credentials = null

    logDebug("appliedCredentials:" + appliedCredentials + ",credentials:" + credentials)

    if (!appliedCredentials && credentials != null) {
      appliedCredentials = true

      @transient val ugi = UserGroupInformation.getCurrentUser
      ugi.addCredentials(credentials)
      // specify that this is a proxy user
      ugi.setAuthenticationMethod(AuthenticationMethod.PROXY)

      ugi.addCredentials(credentialsConf.value.value)
    }
  }

  /**
   * This function will use the native HBase TableInputFormat with the
   * given scan object to generate a new RDD
   *
   * @param tableName the name of the table to scan
   * @param scan      the HBase scan object to use to read data from HBase
   * @param f         function to convert a Result object from HBase into
   *                  what the user wants in the final generated RDD
   * @return new RDD with results from scan
   */
  def hbaseRDD[U: ClassTag](tableName: TableName, scan: Scan,
                            f: ((ImmutableBytesWritable, Result)) => U): RDD[U] = {

    val job: Job = Job.getInstance(getConf(broadcastedConf))

    TableMapReduceUtil.initCredentials(job)
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
      classOf[IdentityTableMapper], null, null, job)

    val jconf = new JobConf(job.getConfiguration)
    SparkHadoopUtil.get.addCredentials(jconf)
    val rdd = new NewHBaseRDD(_hbaseSession.sparkContext,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result],
      job.getConfiguration,
      this)
    rdd.map(f)
  }

  /**
   * A overloaded version of HBaseContext hbaseRDD that defines the
   * type of the resulting RDD
   *
   * @param tableName the name of the table to scan
   * @param scans     the HBase scan object to use to read data from HBase
   * @return New RDD with results from scan
   *
   */
  def hbaseRDD(tableName: TableName, scans: Scan): RDD[(ImmutableBytesWritable, Result)] = {
    hbaseRDD[(ImmutableBytesWritable, Result)](
      tableName,
      scans,
      (r: (ImmutableBytesWritable, Result)) => r)
  }


  private def getConf(configBroadcast: Broadcast[SerializableWritable[Configuration]]): Configuration = {

    if (tmpHdfsConfiguration == null && tmpHdfsConfgFile != null) {
      val fs = FileSystem.newInstance(SparkHadoopUtil.get.conf)
      val inputStream = fs.open(new Path(tmpHdfsConfgFile))
      tmpHdfsConfiguration = new Configuration(false)
      tmpHdfsConfiguration.readFields(inputStream)
      inputStream.close()
    }

    if (tmpHdfsConfiguration == null) {
      try {
        tmpHdfsConfiguration = configBroadcast.value.value
      } catch {
        case ex: Exception => logError("Unable to getConfig from broadcast", ex)
      }
    }
    tmpHdfsConfiguration
  }

  /**
   * underlining wrapper all get mapPartition functions in HBaseContext
   */
  private class GetMapPartition[T, U](tableName: TableName,
                                      batchSize: Integer,
                                      makeGet: (T) => Get,
                                      convertResult: (Result) => U)
    extends Serializable {

    val tName = tableName.getName

    def run(iterator: Iterator[T], connection: Connection): Iterator[U] = {
      val table = connection.getTable(TableName.valueOf(tName))

      val gets = new java.util.ArrayList[Get]()
      var res = List[U]()

      while (iterator.hasNext) {
        gets.add(makeGet(iterator.next()))

        if (gets.size() == batchSize) {
          val results = table.get(gets)
          res = res ++ results.map(convertResult)
          gets.clear()
        }
      }
      if (gets.size() > 0) {
        val results = table.get(gets)
        res = res ++ results.map(convertResult)
        gets.clear()
      }
      table.close()
      res.iterator
    }
  }

  /**
   * Produces a ClassTag[T], which is actually just a casted ClassTag[AnyRef].
   *
   * This method is used to keep ClassTags out of the external Java API, as
   * the Java compiler cannot produce them automatically. While this
   * ClassTag-faking does please the compiler, it can cause problems at runtime
   * if the Scala API relies on ClassTags for correctness.
   *
   * Often, though, a ClassTag[AnyRef] will not lead to incorrect behavior,
   * just worse performance or security issues.
   * For instance, an Array of AnyRef can hold any type T, but may lose primitive
   * specialization.
   */
  private[spark]
  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

}

object LatestHBaseContextCache {
  def withHBaseExternalCatalog(sc: SparkContext): SparkContext = {
    sc.conf.set(StaticSQLConf.CATALOG_IMPLEMENTATION.key, "hbase")
    sc
  }

  var latest: HBaseSQLContext = _
}

class HBaseSession(
                    @transient val sc: SparkContext,
                    @transient val config: Configuration,
                    @transient extraConfig: Map[String, String]) extends SparkSession(sc) {
  self =>
  @transient
  override lazy val sessionState: SessionState = {
    new HBaseSessionBuilder(this, None).build()
  }

  override def newSession(): HBaseSession = {
    new HBaseSession(sc, config, extraConfig)
  }

  @transient override lazy val catalog: Catalog = new HBaseCatalogImpl(self)

  @transient override lazy val sharedState: SharedState = new HBaseSharedState(sc, initialSessionOptions, extraConfig)

  override val sqlContext: HBaseSQLContext = new HBaseSQLContext(this, config, extraConfig)

}

private[hbase] class HBaseSharedState(
                                       val sc: SparkContext,
                                       initialConfigs: scala.collection.Map[String, String],
                                       extraConfig: Map[String, String])
  extends SharedState(sc, initialConfigs) {

  override lazy val externalCatalog: ExternalCatalogWithListener = {
    val externalCatalog = new HBaseExternalCatalog(
      sc.conf,
      HBaseConfiguration.create(),
      extraConfig)
    val wrapped = new ExternalCatalogWithListener(externalCatalog)
    wrapped.addListener((event: ExternalCatalogEvent) => sparkContext.listenerBus.post(event))
    wrapped
  }

}