package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.hbase.client.HBaseClientImpl
import org.apache.spark.sql.types.StructType

/**
  * Created by wpy on 17-5-16.
  */
class HBaseExternalCatalog(conf: SparkConf, hadoopConf: Configuration)
  extends ExternalCatalog with Logging {

  import CatalogTypes.TablePartitionSpec

  //TODO get it from config
  //  val client: HBaseClient = IsolatedClientLoader.forVersion("2.0", "2.7.1", conf, hadoopConf).createClient()
  val client = new HBaseClientImpl(conf, hadoopConf)

  override protected def doCreateDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    client.createDatabase(dbDefinition, ignoreIfExists)
  }

  override protected def doDropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    client.dropDatabase(db, ignoreIfNotExists, cascade)
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    client.alterDatabase(dbDefinition)
  }

  override def getDatabase(db: String): CatalogDatabase = {
    client.getDatabase(db)
  }

  override def databaseExists(db: String): Boolean = {
    client.databaseExists(db)
  }

  override def listDatabases(): Seq[String] = {
    client.listDatabases(".*")
  }

  override def listDatabases(pattern: String): Seq[String] = {
    client.listDatabases(pattern)
  }

  override def setCurrentDatabase(db: String): Unit = {
    client.setCurrentDatabase(db)
  }

  override protected def doCreateTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    client.createTable(tableDefinition, ignoreIfExists)
  }

  override protected def doDropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = {
    client.dropTable(db, table, ignoreIfNotExists, purge)
  }

  override protected def doRenameTable(db: String, oldName: String, newName: String): Unit = {
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = {
  }

  override def alterTableSchema(db: String, table: String, schema: StructType): Unit = {
  }

  override def getTable(db: String, table: String): CatalogTable = {
    client.getTable(db, table)
  }

  /*override def getTableOption(db: String, table: String): Option[CatalogTable] = {
    client.getTableOption(db, table)
  }*/

  override def tableExists(db: String, table: String): Boolean = {
    client.tableExists(db, table)
  }

  override def listTables(db: String): Seq[String] = {
    client.listTables(db)
  }

  override def listTables(db: String, pattern: String = ".*"): Seq[String] = {
    if (pattern == "*")
      listTables(db).filter(_.matches("." + pattern))
    else listTables(db).filter(_.matches(pattern))
  }

  override def loadTable(db: String, table: String, loadPath: String, isOverwrite: Boolean, isSrcLocal: Boolean): Unit = {
    client.loadTable(loadPath, db + ":" + table, isOverwrite, isSrcLocal)
  }

  import scala.collection.JavaConverters._

  override def loadPartition(db: String, table: String, loadPath: String, partition: TablePartitionSpec, isOverwrite: Boolean, inheritTableSpecs: Boolean, isSrcLocal: Boolean): Unit = {
    client.loadPartition(db, table, loadPath, partition.seq.asJava.asInstanceOf[java.util.LinkedHashMap[String, String]], isOverwrite, inheritTableSpecs, isSrcLocal)
  }

  override def loadDynamicPartitions(db: String, table: String, loadPath: String, partition: TablePartitionSpec, replace: Boolean, numDP: Int): Unit = {
    client.loadDynamicPartitions(db, table, loadPath, partition.seq.asJava.asInstanceOf[java.util.LinkedHashMap[String, String]], replace, numDP)
  }

  override def createPartitions(db: String, table: String, parts: Seq[CatalogTablePartition], ignoreIfExists: Boolean): Unit = {
    client.createPartitions(db, table, parts, ignoreIfExists)
  }

  override def dropPartitions(db: String, table: String, parts: Seq[TablePartitionSpec], ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = {
    client.dropPartitions(db, table, parts, ignoreIfNotExists, purge, retainData)
  }

  override def renamePartitions(db: String, table: String, specs: Seq[TablePartitionSpec], newSpecs: Seq[TablePartitionSpec]): Unit = {
    client.renamePartitions(db, table, specs, newSpecs)
  }

  override def alterPartitions(db: String, table: String, parts: Seq[CatalogTablePartition]): Unit = {
    client.alterPartitions(db, table, parts)
  }

  override def getPartition(db: String, table: String, spec: TablePartitionSpec): CatalogTablePartition = {
    client.getPartition(db, table, spec)
  }

  override def getPartitionOption(db: String, table: String, spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    client.getPartitionOption(db, table, spec)
  }

  override def listPartitionNames(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[String] = {
    throw new UnsupportedOperationException("listPartitionNames")
  }

  override def listPartitions(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = {
    throw new UnsupportedOperationException("listPartitions")
  }

  override def listPartitionsByFilter(db: String, table: String, predicates: Seq[Expression], defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    throw new UnsupportedOperationException("listPartitionsByFilter")
  }

  override protected def doCreateFunction(db: String, funcDefinition: CatalogFunction): Unit = {
    throw new UnsupportedOperationException("doCreateFunction")
  }

  override protected def doDropFunction(db: String, funcName: String): Unit = {
    throw new UnsupportedOperationException("doDropFunction")
  }

  override protected def doRenameFunction(db: String, oldName: String, newName: String): Unit = {
    throw new UnsupportedOperationException("doRenameFunction")
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = {
    throw new UnsupportedOperationException("getFunction")
  }

  override def functionExists(db: String, funcName: String): Boolean = {
    throw new UnsupportedOperationException("functionExists")
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = {
    throw new UnsupportedOperationException("listFunctions")
  }

  override def alterTableStats(db: String, table: String, stats: Option[CatalogStatistics]): Unit = {
    throw new UnsupportedOperationException("alterTableStats")
  }

  override protected def doAlterFunction(db: String, funcDefinition: CatalogFunction): Unit = {
    throw new UnsupportedOperationException("doAlterFunction")
  }
}

object HBaseExternalCatalog {
  // If defined and larger than 3, a new table will be created with the nubmer of region specified.
  val newTable = "newtable"
  // The json string specifying hbase catalog information
  val regionStart = "regionStart"
  val defaultRegionStart = "aaaaaaa"
  val regionEnd = "regionEnd"
  val defaultRegionEnd = "zzzzzzz"
  val tableCatalog = "catalog"
  // The row key with format key1:key2 specifying table row key
  val rowKey = "rowkey"
  // The key for hbase table whose value specify namespace and table name
  val table = "table"
  // The namespace of hbase table
  val nameSpace = "namespace"
  // The name of hbase table
  val tableName = "name"
  // The name of columns in hbase catalog
  val columns = "columns"
  val cf = "cf"
  val col = "col"
  val `type` = "type"
  // the name of avro schema json string
  val avro = "avro"
  val delimiter: Byte = 0
  val serdes = "serdes"
  val length = "length"

  val SPARK_SQL_PREFIX = "spark.sql."

  val TABLE_KEY: String = "hbase.table"
  val SCHEMA_COLUMNS_MAPPING_KEY: String = "hbase.columns.mapping"
}