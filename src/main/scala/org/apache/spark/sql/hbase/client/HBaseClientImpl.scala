package org.apache.spark.sql.hbase.client

import java.io.{FileInputStream, PrintStream}
import java.net.URI
import java.util.Properties
import java.{util => ju}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
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
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.CircularBuffer
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._

/**
  * Created by wpy on 2017/5/12.
  */
class HBaseClientImpl(
                       override val version: HBaseVersion,
                       sparkConf: SparkConf,
                       hadoopConf: Configuration,
                       extraConfig: Map[String, String],
                       initClassLoader: ClassLoader,
                       val clientLoader: IsolatedClientLoader)
  extends HBaseClient
    with Logging {
  def this(sparkConf: SparkConf, hadoopConf: Configuration) {
    this(null, sparkConf, hadoopConf, null, null, null)
  }

  private val schemaPath = {
    val in = getClass.getResourceAsStream("/spark_hbase.properties")
    val props = new Properties()
    props.load(in)
    val url = props.getProperty("schema.file.url")
    in.close()
    url
  }

  // Map[ table, Map[ family, Map[qua_name, type ] ]
  private def getSchemaProp = {
    val yaml = new Yaml()
    val inputStream = new FileInputStream(schemaPath)
    val y = yaml.load[ju.Map[String, ju.Map[String, ju.Map[String, String]]]](inputStream)
    inputStream.close()
    y
  }

  private def getColumnNames(schemaMap: ju.Map[String, ju.Map[String, ju.Map[String, String]]], tableName: String) = {
    "{+\n" + schemaMap.get(tableName).asScala.map { cf =>
      cf._1 + "-> (" + cf._2.asScala.keys.mkString(", ") +
        ")"
    }.mkString(";\n") + "\n}"
  }

  private def getSchema(schemaMap: ju.Map[String, ju.Map[String, ju.Map[String, String]]], tableName: String) = StructType {
    schemaMap.get(tableName).asScala.flatMap { col =>
      val familyName = col._1
      col._2.asScala.map { qualifier =>
        StructField(familyName + "_" + qualifier._1, CatalystSqlParser.parseDataType(qualifier._2))
      }
    }.toArray
  }


  // Circular buffer to hold what hbase prints to STDOUT and ERR.  Only printed when failures occur.
  private val outputBuffer = new CircularBuffer()

  private val conf = HBaseConfiguration.create(hadoopConf)

  private val userName = User.getCurrent

  val conn: Connection = ConnectionFactory.createConnection(conf, userName)

  lazy val adm: Admin = connection.getAdmin

  /** Returns the configuration for the given key in the current session. */
  override def getConf(key: String, defaultValue: String): String = conf.get(key, defaultValue)

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
      throw new NoSuchDatabaseException(name)
  }

  /** Return whether a table/view with the specified name exists. */
  override def databaseExists(dbName: String): Boolean = {
    admin.listNamespaceDescriptors.exists(_.getName == dbName)
  }

  /** List the names of all the databases that match the specified pattern. */
  override def listDatabases(pattern: String): Seq[String] = {
    admin.listNamespaceDescriptors().map(_.getName).filter(_.matches(pattern))
  }

  /** Return whether a table/view with the specified name exists. */
  override def tableExists(dbName: String, tableName: String): Boolean = {
    admin.tableExists(TableName.valueOf(s"$dbName:$tableName"))
  }

  /** Returns the metadata for the specified table or None if it doesn't exist. */
  override def getTableOption(dbName: String, tableName: String): Option[CatalogTable] = {
    val name = s"$dbName:$tableName"
    logDebug(s"Looking up $dbName:$tableName")
    Option(admin.getDescriptor(TableName.valueOf(name))).map { t =>

      //TODO need reality schemas
      val schemaMap = getSchemaProp
      val schema = getSchema(schemaMap, name)
      //TODO should add more properties in future
      val props = {
        val regions = admin.getRegions(TableName.valueOf(name))
        val tableDesc = admin.getDescriptor(TableName.valueOf(name))
        /*
        {cf1->{q1,q2,...,}}
        {cf2->{q1,q2,...,}}
         */
        val cols = tableDesc.getColumnFamilies
        //{CF1: (Q1, Q2, Q3, ... ,Qn)}; {CF2: (Q1, Q2, Q3, ... ,Qn)}; ... ;{CF3: (Q1, Q2, Q3, ... ,Qn)}
        val qualifiers = getColumnNames(schemaMap, name)
        //          .map(cf=>s"{${cf._1}:(${cf._2.mkString(",")})}").mkString(";")
        val encoding = cols.map(family => family.getNameAsString -> Bytes.toString(family.getDataBlockEncoding.getNameInBytes)).mkString(";")
        //"K1, K2, K3, ... ,Kn"
        val splitKeys = regions.iterator().asScala.map(region => Bytes.toString(region.getEndKey)).mkString(",")
        val bloom = cols.map(family => family.getNameAsString + ":" + family.getBloomFilterType.name()).mkString(";")
        val zip = cols.map(family => family.getNameAsString + ":" + family.getCompressionType.getName).mkString(";")
        val columns = cols.map(_.getNameAsString).mkString(",")
        val table = s"$tableName"
        val db = s"$dbName"
        Map("db" -> db, "table" -> table, "qualifiers" -> qualifiers, "columns" -> columns, "encoding" -> encoding, "split" -> splitKeys, "bloom" -> bloom, "zip" -> zip)
      }
      val path = new Path(conf.get("hbase.rootdir") + s"/data/$dbName/$tableName")
      //TODO should add properties like {{cf:{Q1,Q2,...,Qn}}}, {splitKey:{S1,S2,...,Sn}}, {{DataEncoding:{prefix,diff,...}}}, {BloomType:{BloomType}}
      CatalogTable(
        identifier = TableIdentifier(tableName, Option(dbName)),
        //TODO tableType && table schema
        tableType = CatalogTableType.MANAGED,
        schema = schema,
        storage = CatalogStorageFormat(
          locationUri = Option(path.toUri),
          // To avoid ClassNotFound exception, we try our best to not get the format class, but get
          // the class name directly. However, for non-native tables, there is no interface to get
          // the format class name, so we may still throw ClassNotFound in this case.
          inputFormat = None,
          outputFormat = None, serde = None, compressed = false, properties = Map.empty),
        properties = props,
        stats = Some(CatalogStatistics(FileSystem.get(conf).getContentSummary(path).getLength)))
    }
  }

  /** Creates a table with the given metadata. */
  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit = {
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
    if (!databaseExists(dbName)) {
      admin.createNamespace(NamespaceDescriptor.create(dbName).build())
    }
    val props = table.properties
    val tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(table.identifier.table))
    //TODO may be used in "INSERT EXPRESSION"
    // "qualifiers" "{CF1:(Q1, Q2, Q3, ... ,Qn)}; {CF2:(Q1, Q2, Q3, ... ,Qn)}; ... ;{CFn:(Q1, Q2, Q3, ... ,Qn)}"
    //    val cols = props("qualifiers").replaceAll("\t", "").replaceAll(" ", "")

    //"column" "CF1, CF2, CF3, ...,CFn"
    val cols = props("column").replaceAll("\t", "").replaceAll(" ", "")
    //"encoding" "CF1:DataBlockEncoding; CF2:DataBlockEncoding; ... ;CFn:DataBlockEncoding"
    val encoding = propStringToMap(props("encoding"))(toMapFunc)
    //"split" "K1, K2, K3, ... ,Kn"
    val splitKeys = props("split").replaceAll("\t", "").replaceAll(" ", "")
    //"bloom" "CF1: type; CF2: type; CF3: type; ...;CFn: type"
    val bloomType = propStringToMap(props("bloom"))(toMapFunc)
    //"zip" "{CF1, algorithm}; {CF2, algorithm}; {CF3, algorithm}; ... ;{CFn, algorithm}"
    val zip = propStringToMap(props("zip"))(toMapFunc)

    //start get properties & create new table

    //get column_family properties
    val pattern = "\\{([0-9a-zA-Z]*):\\((([0-9a-zA-Z]*([,])?)*)\\)\\}".r

    //unused
    /*val colQualifier = cols.split(";").map{
      case pattern(cf, qualifiers, _*) => {
        val qualifier = qualifiers.split(",").map(Bytes.toBytes)
        qualifier.map(q=>Map(cf->q)).reduce(_++_)
      }
    }*/

    cols.split(";").foreach {
      case pattern(cf, _, _*) => {
        val bloom = bloomType(cf)
        val compression = zip(cf)
        val en = encoding(cf)
        val columnFamily = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf))
        columnFamily.setBlockCacheEnabled(true)
        columnFamily.setDataBlockEncoding(DataBlockEncoding.valueOf(en))
        columnFamily.setCompressionType(Compression.Algorithm.valueOf(compression))
        columnFamily.setBloomFilterType(BloomType.valueOf(bloom))
        tableDesc.setColumnFamily(columnFamily.build())
      }
    }
    val split = splitKeys.split(",").filter(_.nonEmpty).map(Bytes.toBytes)
    //create table
    admin.createTable(tableDesc.build(), split)
  }

  /** Drop the specified table. */
  override def dropTable(dbName: String, tableName: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = {
    val name = TableName.valueOf(dbName + ":" + tableName)
    admin.disableTable(name)
    admin.deleteTable(name)
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
    clientLoader.createClient().asInstanceOf[HBaseClientImpl]
  }

  /** Used for testing only.  Removes all data from this instance of HBase. */
  override def reset(): Unit = {
    admin.listTableNames().foreach(tableName => admin.truncateTable(tableName, true))
  }
}

/**
  * Converts the native table metadata representation format CatalogTable to HBase's Table.
  */
object HBaseClientImpl {
}