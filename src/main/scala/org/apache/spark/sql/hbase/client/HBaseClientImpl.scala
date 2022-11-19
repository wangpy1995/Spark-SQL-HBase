package org.apache.spark.sql.hbase.client

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, NamespaceDescriptor, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.hbase.SparkHBaseConstants.TABLE_CONSTANTS
import org.apache.spark.sql.hbase.execution.{DefaultRowKeyGenerator, HBaseSqlParser, HBaseTableFormat}
import org.apache.spark.sql.hbase.utils.HBaseSparkFormatUtils
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.{CircularBuffer, SerializableConfiguration}
import org.yaml.snakeyaml.Yaml

import java.io.{FileInputStream, FileWriter, ObjectInputStream, PrintStream}
import java.net.URI
import java.{util => ju}
import scala.annotation.meta.param
import scala.collection.JavaConverters._

/**
 * Created by wpy on 2017/5/12.
 */
class HBaseClientImpl(
                       @(transient@param) override val version: HBaseVersion,
                       @(transient@param) sparkConf: SparkConf,
                       @(transient@param) hadoopConf: Configuration,
                       extraConfig: Map[String, String],
                       @(transient@param) initClassLoader: ClassLoader,
                       @(transient@param) val clientLoader: IsolatedClientLoader)
  extends HBaseClient
    with Logging {

  private val schemaPath = extraConfig("schema.file.url")
  private lazy val yaml = new Yaml()

  // Circular buffer to hold what hbase prints to STDOUT and ERR.  Only printed when failures occur.
  @transient private var outputBuffer = new CircularBuffer()

  private val serializedConf = new SerializableConfiguration(hadoopConf)

  @transient private var conf = HBaseConfiguration.create(serializedConf.value)

  @transient private var userName = User.getCurrent

  @transient var conn: Connection = ConnectionFactory.createConnection(conf, userName)

  lazy val adm: Admin = connection.getAdmin

  /*  logInfo(
      s"Root directory location for HBase client " +
        s"(version ${version.fullVersion}) is ${Option(new Path(conf.get("hbase.rootdir")).toUri)}")*/
  private val retryLimit = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 31)
  private val retryDelayMillis = conf.getLong(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 12000)

  /**
   * Runs `f` with multiple retries in case the hbase metastore is temporarily unreachable.
   */
  private def retryLocked[A](f: => A): A = synchronized {
    // HBase sometimes retries internally, so set a deadline to avoid compounding delays.
    val deadline = System.nanoTime + (retryLimit * retryDelayMillis * 1e6).toLong
    var numTries = 0
    var caughtException: Exception = null
    do {
      numTries += 1
      try {
        return f
      } catch {
        case e: Exception if causedByThrift(e) =>
          caughtException = e
          logWarning(
            "HBaseClient got thrift exception, destroying client and retrying " +
              s"(${retryLimit - numTries} tries remaining)", e)
          clientLoader.cachedConnection = null
          Thread.sleep(retryDelayMillis)
      }
    } while (numTries <= retryLimit && System.nanoTime < deadline)
    if (System.nanoTime > deadline) {
      logWarning("Deadline exceeded")
    }
    throw caughtException
  }

  private def causedByThrift(e: Throwable): Boolean = {
    var target = e
    while (target != null) {
      val msg = target.getMessage
      if (msg != null && msg.matches("(?s).*(TApplication|TProtocol|TTransport)Exception.*")) {
        return true
      }
      target = target.getCause
    }
    false
  }

  /** Returns the configuration for the given key in the current session. */
  override def getConf(key: String, defaultValue: String): String = conf.get(key, defaultValue)

  /**
   * 读取yml文件, 每次getTable时都会重新读取
   *
   * @return
   */
  private def getSchemaProp = {
    // Map[tableName, Map[colum:qua_name, type]
    val inputStream = new FileInputStream(schemaPath)
    val y = yaml.load[ju.Map[String, ju.Map[String, String]]](inputStream)
    inputStream.close()
    y
  }

  private def addTableSchema(table: CatalogTable): Unit = synchronized{
    // Map[tableName, Map[colum:qua_name, type]
    val schemaMap = getSchemaProp
    val tableMap: ju.Map[String, String] = new ju.LinkedHashMap()
    //row_key
    val keyType = table.schema(TABLE_CONSTANTS.ROW_KEY.getValue).dataType.typeName
    tableMap.put(TABLE_CONSTANTS.ROW_KEY.getValue, keyType)
    //generator
    val generator = table.properties.getOrElse("generator", classOf[DefaultRowKeyGenerator].getCanonicalName)
    tableMap.put("generator", generator)
    //columns
    table.schema.filter(_.name != TABLE_CONSTANTS.ROW_KEY.getValue).foreach { field =>
      tableMap.put(field.name, field.dataType.typeName)
    }
    //增加新表的Schema
    schemaMap.put(table.database + ":" + table.identifier.table, tableMap)
    val writer = new FileWriter(schemaPath, false)
    yaml.dump(schemaMap, writer)
    writer.close()
  }

  private def dropTableSchema(dbName:String, tableName: String):Unit=synchronized{
    val schemaMap = getSchemaProp
    schemaMap.remove(dbName + ":" + tableName)
    val writer = new FileWriter(schemaPath, false)
    yaml.dump(schemaMap, writer)
    writer.close()
  }

  private def getColumnQualifiers(schemaMap: ju.Map[String, ju.Map[String, String]], tableName: String) = {
    yaml.dump(schemaMap.get(tableName))
  }

  /**
   * 获取各个qualifier对应的数据类型
   *
   * @param schemaMap 表结构
   * @param tableName 表名
   * @return
   */
  private def getSchema(schemaMap: ju.Map[String, ju.Map[String, String]], tableName: String) = StructType {
    val tableSchema = schemaMap.get(tableName)
    tableSchema.asScala.map { case (col_name, dataType) =>
      //family和qualifier字段的名字用“:”组合
      StructField(col_name, HBaseSqlParser.parseDataType(dataType))
    }.toList
  }


  def connection: Connection = {
    if (clientLoader != null) {
      if (clientLoader.cachedConnection != null) {
        clientLoader.cachedConnection.asInstanceOf[Connection]
      } else {
        val c = conn
        clientLoader.cachedConnection = c
        c
      }
    } else {
      conn
    }
  }

  def admin: Admin = {
    if (clientLoader != null) {
      if (clientLoader.cachedAdmin != null) {
        clientLoader.cachedAdmin.asInstanceOf[Admin]
      } else {
        val a = adm
        clientLoader.cachedAdmin = a
        a
      }
    } else adm
  }

  override def setOut(stream: PrintStream): Unit = {
    new PrintStream(outputBuffer, true, "UTF-8")
  }

  override def setInfo(stream: PrintStream): Unit = {
    new PrintStream(outputBuffer, true, "UTF-8")
  }

  override def setError(stream: PrintStream): Unit = {
    new PrintStream(outputBuffer, true, "UTF-8")
  }

  def getQualifiedTableName(tableName: String): String = {
    val name = tableName.split(":")
    if (name.length > 1) name(1)
    else tableName
  }

  /** Returns the names of all tables in the given database. */
  override def listTables(dbName: String): Seq[String] = {
    admin.listTableNamesByNamespace(dbName).map(name => getQualifiedTableName(name.getNameAsString))
  }

  /** Returns the names of tables in the given database that matches the given pattern. */
  override def listTables(dbName: String, pattern: String): Seq[String] = {
    listTables(dbName).filter(_.matches(pattern))
  }

  /** Sets the name of current database. */
  override def setCurrentDatabase(databaseName: String): Unit = {
    throw new UnsupportedOperationException(s"setCurrentDatabase($databaseName)")
  }

  /** Returns the metadata for specified database, throwing an exception if it doesn't exist */
  override def getDatabase(name: String): CatalogDatabase = {
    if (databaseExists(name))
      CatalogDatabase(name, name + " description", new URI(conf.get("hbase.rootdit") + s"/data/$name"), Map.empty)
    else
      throw NoSuchDatabaseException(name)
  }

  /** Return whether a table/view with the specified name exists. */
  override def databaseExists(dbName: String): Boolean = {
    admin.listNamespaceDescriptors.exists(_.getName == dbName)
  }

  /** List the names of all the databases that match the specified pattern. */
  override def listDatabases(pattern: String): Seq[String] = {
    admin.listNamespaceDescriptors().map(_.getName).filter(_.matches(pattern))
  }

  /** Returns the metadata for the specified table or None if it doesn't exist. */
  override def getTableOption(dbName: String, tableName: String): Option[CatalogTable] = {
    val table = s"$dbName:$tableName"
    logDebug(s"Looking up $dbName:$tableName")
    Option(admin.getDescriptor(TableName.valueOf(table))).map { t =>

      //TODO need reality schemas
      val schemaMap = getSchemaProp
      //获取并remove该表的rowKey生成器
      val rowKeyGeneratorName = schemaMap.get(table).remove("generator")
      val schema = getSchema(schemaMap, table)

      //TODO should add more properties in future
      val props = {
        val regions = admin.getRegions(TableName.valueOf(table))
        val tableDesc = admin.getDescriptor(TableName.valueOf(table))
        val cols = tableDesc.getColumnFamilies
        /** Map{
         * 'CF1:Q1': dataType1,
         * ....... ,
         * 'CF1:Qn': dataTypeN}
         * */
        val qualifiers = getColumnQualifiers(schemaMap, table)
        val encoding = yaml.dump(cols.map(family => family.getNameAsString -> Bytes.toString(family.getDataBlockEncoding.getNameInBytes)).toMap.asJava)
        //"K1, K2, K3, ... ,Kn"
        val splitKeys = yaml.dump(regions.iterator().asScala.map(region => Bytes.toString(region.getEndKey)).asJava)
        val bloom = yaml.dump(cols.map(family => family.getNameAsString -> family.getBloomFilterType.name()).toMap.asJava)
        val zip = yaml.dump(cols.map(family => family.getNameAsString -> family.getCompressionType.getName).toMap.asJava)
        val columns = cols.map(_.getNameAsString).mkString(",")
        Map("db" -> s"$dbName",
          "table" -> s"$tableName",
          "qualifiers" -> qualifiers,
          "columns" -> columns,
          "encoding" -> encoding,
          "split" -> splitKeys,
          "bloom" -> bloom,
          "zip" -> zip,
          "generator" -> rowKeyGeneratorName)
      }
      val path = new Path(conf.get("hbase.rootdir") + s"/data/$dbName/$tableName")
      //TODO should add properties like {{cf:{Q1,Q2,...,Qn}}}, {splitKey:{S1,S2,...,Sn}}, {{DataEncoding:{prefix,diff,...}}}, {BloomType:{BloomType}}
      CatalogTable(
        identifier = TableIdentifier(tableName, Option(dbName)),
        //TODO tableType && table schema
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat(
          locationUri = Option(path.toUri),
          // To avoid ClassNotFound exception, we try our best to not get the format class, but get
          // the class name directly. However, for non-native tables, there is no interface to get
          // the format class name, so we may still throw ClassNotFound in this case.
          inputFormat = Some(classOf[TableInputFormat].getTypeName),
          outputFormat = Some(classOf[(ImmutableBytesWritable, Result)].getTypeName),
          serde = None,
          compressed = false,
          properties = Map.empty),
        schema = schema,
        //TODO 可以用来关联HBase外部数据源
        provider = Some(classOf[HBaseTableFormat].getCanonicalName),
        properties = props,
        stats = Some(CatalogStatistics(FileSystem.get(conf).getContentSummary(path).getLength)))
    }
  }

  /** Return whether a table/view with the specified name exists. */
  override def tableExists(dbName: String, tableName: String): Boolean = {
    admin.tableExists(TableName.valueOf(s"$dbName:$tableName"))
  }

  /** Creates a table with the given metadata. */
  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val exists = tableExists(table.database, table.identifier.table)
    if (exists && ignoreIfExists) {
      logInfo(s"table ${table.database}:${table.qualifiedName} is existed, create table is ignored")
    } else {
      def propStringToMap(propStr: String)(f: String => Map[String, String]): Map[String, String] = {
        f(propStr)
      }

      def toMapFunc: String => Map[String, String] = { a =>
        a.replaceAll("\t", "").replaceAll(" ", "").split(";").map { b =>
          val r = b.split(":")
          Map(r(0) -> r(1))
        }.reduce(_ ++ _)
      }

      val dbName = table.identifier.database.get
      // dbName对应hbase的namespace
      if (!databaseExists(dbName)) {
        admin.createNamespace(NamespaceDescriptor.create(dbName).build())
      }
      //建表语句如果是CREATE TABLE target_table_identifier LIKE source_table_identifier USING xxxx形式时,不指定properties
      val tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(s"$dbName:${table.identifier.table}"))
      table.schema.map { filed =>
        val separateName = HBaseSparkFormatUtils.splitColumnAndQualifierName(filed.name)
        separateName.familyName
      }.toSet.filter(_ != TABLE_CONSTANTS.ROW_KEY.getValue).foreach { familyName =>
        val columnFamily = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName))
        columnFamily.setBlockCacheEnabled(true)
        //TODO 从properties中读取DataBlockEncoding CompressionType 信息
        tableDesc.setColumnFamily(columnFamily.build())
      }
      //create table
      if(exists) {
        dropTable(dbName, table.identifier.table, ignoreIfExists, purge = false)
      }
      //当properties中指定了splitKey
      if(table.properties.contains("splitKeys")){
        val splitKeys = table.properties("splitKeys").split(",").map(Bytes.toBytes)
        admin.createTable(tableDesc.build(),splitKeys)
      }else{
        admin.createTable(tableDesc.build())
      }

      //将schema信息写入到文件中
      addTableSchema(table)
    }
  }

  /** Drop the specified table. */
  override def dropTable(dbName: String, tableName: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = {
    val name = TableName.valueOf(dbName + ":" + tableName)
    admin.disableTable(name)
    admin.deleteTable(name)
    //从yaml文件中删除对应的Schema
    dropTableSchema(dbName ,tableName)
  }

  /** Updates the given table with new metadata, optionally renaming the table. */
  override def alterTable(tableName: String, table: CatalogTable): Unit = {
    throw new UnsupportedOperationException("alterTable")
  }

  /** Creates a new database with the given name. */
  override def createDatabase(database: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    if (!databaseExists(database.name)) {
      admin.createNamespace(NamespaceDescriptor.create(database.name).build())
    } else if (!ignoreIfExists) {
      admin.deleteNamespace(database.name)
      createDatabase(database, ignoreIfExists)
    }
  }

  /**
   * Drop the specified database, if it exists.
   *
   * @param name              database to drop
   * @param ignoreIfNotExists if true, do not throw error if the database does not exist
   * @param cascade           whether to remove all associated objects such as tables and functions
   */
  override def dropDatabase(name: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    admin.deleteNamespace(name)
  }

  /**
   * Alter a database whose name matches the one specified in `database`, assuming it exists.
   */
  override def alterDatabase(database: CatalogDatabase): Unit = {
    admin.modifyNamespace(NamespaceDescriptor.create(database.name).build())
  }

  /**
   * Create one or many partitions in the given table.
   */
  override def createPartitions(db: String, table: String, parts: Seq[CatalogTablePartition], ignoreIfExists: Boolean): Unit = {
    throw new UnsupportedOperationException("createPartitions")
  }

  /**
   * Drop one or many partitions in the given table, assuming they exist.
   */
  override def dropPartitions(db: String, table: String, specs: Seq[TablePartitionSpec], ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = {
    throw new UnsupportedOperationException("dropPartitions")
  }

  /**
   * Rename one or many existing table partitions, assuming they exist.
   */
  override def renamePartitions(db: String, table: String, specs: Seq[TablePartitionSpec], newSpecs: Seq[TablePartitionSpec]): Unit = {
    throw new UnsupportedOperationException("renamePartitions")
  }

  /**
   * Alter one or more table partitions whose specs match the ones specified in `newParts`,
   * assuming the partitions exist.
   */
  override def alterPartitions(db: String, table: String, newParts: Seq[CatalogTablePartition]): Unit = {
    throw new UnsupportedOperationException("alterPartitions")
  }

  /**
   * Returns the partition names for the given table that match the supplied partition spec.
   * If no partition spec is specified, all partitions are returned.
   *
   * The returned sequence is sorted as strings.
   */
  override def getPartitionNames(table: CatalogTable, partialSpec: Option[TablePartitionSpec]): Seq[String] = {
    throw new UnsupportedOperationException("getPartitionNames")
  }

  /** Returns the specified partition or None if it does not exist. */
  override def getPartitionOption(table: CatalogTable, spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    throw new UnsupportedOperationException("getPartitionOption")
  }

  /**
   * Returns the partitions for the given table that match the supplied partition spec.
   * If no partition spec is specified, all partitions are returned.
   */
  override def getPartitions(catalogTable: CatalogTable, partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = {
    throw new UnsupportedOperationException("getPartition")
  }

  /** Returns partitions filtered by predicates for the given table. */
  override def getPartitionsByFilter(catalogTable: CatalogTable, predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    throw new UnsupportedOperationException("getPartitionsByFilter")
  }

  /** Loads a static partition into an existing table. */
  override def loadPartition(loadPath: String, dbName: String, tableName: String, partSpec: ju.LinkedHashMap[String, String], replace: Boolean, inheritTableSpecs: Boolean, isSrcLocal: Boolean): Unit = {
    throw new UnsupportedOperationException("loadPartition")
  }

  /** Loads data into an existing table. */
  override def loadTable(loadPath: String, tableName: String, replace: Boolean, isSrcLocal: Boolean): Unit = {
    //TODO may supported in the future
    throw new UnsupportedOperationException("loadTable")
  }

  /** Loads new dynamic partitions into an existing table. */
  override def loadDynamicPartitions(loadPath: String, dbName: String, tableName: String, partSpec: ju.LinkedHashMap[String, String], replace: Boolean, numDP: Int): Unit = {
    throw new UnsupportedOperationException("loadDynamicPartitions")
  }

  /** Add a jar into class loader */
  override def addJar(path: String): Unit = {
    throw new UnsupportedOperationException("addJar")
  }

  /** Return a [[HBaseClient]] as new session, that will share the class loader and HBase client */
  override def newSession(): HBaseClient = {
    clientLoader.createClient()
  }

  /** Used for testing only.  Removes all data from this instance of HBase. */
  override def reset(): Unit = {
    admin.listTableNames().foreach(tableName => admin.truncateTable(tableName, true))
  }

  def readObject(input: ObjectInputStream): Unit = {
    outputBuffer = new CircularBuffer()
    userName = User.getCurrent
    conf = serializedConf.value
    conn = ConnectionFactory.createConnection(conf, userName)
  }
}

/**
 * Converts the native table metadata representation format CatalogTable to HBase's Table.
 */
object HBaseClientImpl {
}