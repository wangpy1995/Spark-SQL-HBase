package org.apache.spark.sql.hbase

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.catalog.{Catalog, CatalogMetadata, Column, Database, Function, Table}
import org.apache.spark.sql.catalyst.analysis.UnresolvedTable
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, RecoverPartitions}
import org.apache.spark.sql.catalyst.{DefinedByConstructorParams, FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.connector.catalog.{NamespaceChange, SupportsNamespaces}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource}
import org.apache.spark.sql.hbase.utils.StructFieldConverters.toAttributes
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.StorageLevel

import java.net.URI
import java.util
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

/**
 * Created by wpy on 17-5-18.
 */
class HBaseCatalogImpl(hbaseSession: HBaseSession) extends Catalog with SupportsNamespaces {

  import org.apache.spark.sql.Dataset

  import scala.collection.JavaConverters._

  private def sessionCatalog: SessionCatalog = hbaseSession.sessionState.catalog

  private def requireDatabaseExists(dbName: String): Unit = {
    if (!sessionCatalog.databaseExists(dbName)) {
      throw new AnalysisException(s"Database '$dbName' does not exist.")
    }
  }

  private def requireTableExists(dbName: String, tableName: String): Unit = {
    if (!sessionCatalog.tableExists(TableIdentifier(tableName, Some(dbName)))) {
      throw new AnalysisException(s"Table '$tableName' does not exist in database '$dbName'.")
    }
  }

  /**
   * Returns the current default database in this session.
   */
  override def currentDatabase: String = sessionCatalog.getCurrentDatabase

  /**
   * Sets the current default database in this session.
   */
  @throws[AnalysisException]("database does not exist")
  override def setCurrentDatabase(dbName: String): Unit = {
    requireDatabaseExists(dbName)
    sessionCatalog.setCurrentDatabase(dbName)
  }

  /**
   * Returns a list of databases available across all sessions.
   */
  override def listDatabases(): Dataset[Database] = {
    val databases = sessionCatalog.listDatabases().map(makeDatabase)
    HBaseCatalogImpl.makeDataset(databases, hbaseSession)
  }

  private def makeDatabase(dbName: String): Database = {
    val metadata = sessionCatalog.getDatabaseMetadata(dbName)
    new Database(
      name = metadata.name,
      description = metadata.description,
      locationUri = CatalogUtils.URIToString(metadata.locationUri))
  }

  /**
   * Returns a list of tables in the current database.
   * This includes all temporary tables.
   */
  override def listTables(): Dataset[Table] = {
    listTables(currentDatabase)
  }

  /**
   * Returns a list of tables in the specified database.
   * This includes all temporary tables.
   */
  @throws[AnalysisException]("database does not exist")
  override def listTables(dbName: String): Dataset[Table] = {
    val tables = sessionCatalog.listTables(dbName).map(makeTable)
    HBaseCatalogImpl.makeDataset(tables, hbaseSession)
  }

  /**
   * Returns a Table for the given table/view or temporary view.
   *
   * Note that this function requires the table already exists in the Catalog.
   *
   * If the table metadata retrieval failed due to any reason (e.g., table serde class
   * is not accessible or the table type is not accepted by Spark SQL), this function
   * still returns the corresponding Table without the description and tableType)
   */
  private def makeTable(tableIdent: TableIdentifier): Table = {
    val metadata = try {
      Some(sessionCatalog.getTempViewOrPermanentTableMetadata(tableIdent))
    } catch {
      case NonFatal(_) => None
    }
    val isTemp = sessionCatalog.isTempView(tableIdent)
    new Table(
      name = tableIdent.table,
      database = metadata.map(_.identifier.database).getOrElse(tableIdent.database).orNull,
      description = metadata.map(_.comment.orNull).orNull,
      tableType = if (isTemp) "TEMPORARY" else metadata.map(_.tableType.name).orNull,
      isTemporary = isTemp)
  }

  /**
   * Returns a list of functions registered in the current database.
   * This includes all temporary functions
   */
  override def listFunctions(): Dataset[Function] = {
    listFunctions(currentDatabase)
  }

  /**
   * Returns a list of functions registered in the specified database.
   * This includes all temporary functions
   */
  @throws[AnalysisException]("database does not exist")
  override def listFunctions(dbName: String): Dataset[Function] = {
    requireDatabaseExists(dbName)
    val functions = sessionCatalog.listFunctions(dbName).map { case (functIdent, _) =>
      makeFunction(functIdent)
    }
    HBaseCatalogImpl.makeDataset(functions, hbaseSession)
  }

  private def makeFunction(funcIdent: FunctionIdentifier): Function = {
    val metadata = sessionCatalog.lookupFunctionInfo(funcIdent)
    new Function(
      name = metadata.getName,
      database = metadata.getDb,
      description = null, // for now, this is always undefined
      className = metadata.getClassName,
      isTemporary = metadata.getDb == null)
  }

  /**
   * Returns a list of columns for the given table/view or temporary view.
   */
  @throws[AnalysisException]("table does not exist")
  override def listColumns(tableName: String): Dataset[Column] = {
    val tableIdent = hbaseSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    listColumns(tableIdent)
  }

  /**
   * Returns a list of columns for the given table/view or temporary view in the specified database.
   */
  @throws[AnalysisException]("database or table does not exist")
  override def listColumns(dbName: String, tableName: String): Dataset[Column] = {
    requireTableExists(dbName, tableName)
    listColumns(TableIdentifier(tableName, Some(dbName)))
  }

  private def listColumns(tableIdentifier: TableIdentifier): Dataset[Column] = {
    val tableMetadata = sessionCatalog.getTempViewOrPermanentTableMetadata(tableIdentifier)

    val partitionColumnNames = tableMetadata.partitionColumnNames.toSet
    val bucketColumnNames = tableMetadata.bucketSpec.map(_.bucketColumnNames).getOrElse(Nil).toSet
    val columns = tableMetadata.schema.map { c =>
      new Column(
        name = c.name,
        description = c.getComment().orNull,
        dataType = c.dataType.catalogString,
        nullable = c.nullable,
        isPartition = partitionColumnNames.contains(c.name),
        isBucket = bucketColumnNames.contains(c.name))
    }
    HBaseCatalogImpl.makeDataset(columns, hbaseSession)
  }

  /**
   * Gets the database with the specified name. This throws an `AnalysisException` when no
   * `Database` can be found.
   */
  override def getDatabase(dbName: String): Database = {
    makeDatabase(dbName)
  }

  /**
   * Gets the table or view with the specified name. This table can be a temporary view or a
   * table/view. This throws an `AnalysisException` when no `Table` can be found.
   */
  override def getTable(tableName: String): Table = {
    val tableIdent = hbaseSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    getTable(tableIdent.database.orNull, tableIdent.table)
  }

  /**
   * Gets the table or view with the specified name in the specified database. This throws an
   * `AnalysisException` when no `Table` can be found.
   */
  override def getTable(dbName: String, tableName: String): Table = {
    if (tableExists(dbName, tableName)) {
      makeTable(TableIdentifier(tableName, Option(dbName)))
    } else {
      throw new AnalysisException(s"Table or view '$tableName' not found in database '$dbName'")
    }
  }

  /**
   * Gets the function with the specified name. This function can be a temporary function or a
   * function. This throws an `AnalysisException` when no `Function` can be found.
   */
  override def getFunction(functionName: String): Function = {
    val functionIdent = hbaseSession.sessionState.sqlParser.parseFunctionIdentifier(functionName)
    getFunction(functionIdent.database.orNull, functionIdent.funcName)
  }

  /**
   * Gets the function with the specified name. This returns `None` when no `Function` can be
   * found.
   */
  override def getFunction(dbName: String, functionName: String): Function = {
    makeFunction(FunctionIdentifier(functionName, Option(dbName)))
  }

  /**
   * Checks if the database with the specified name exists.
   */
  override def databaseExists(dbName: String): Boolean = {
    sessionCatalog.databaseExists(dbName)
  }

  /**
   * Checks if the table or view with the specified name exists. This can either be a temporary
   * view or a table/view.
   */
  override def tableExists(tableName: String): Boolean = {
    val tableIdent = hbaseSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    tableExists(tableIdent.database.orNull, tableIdent.table)
  }

  /**
   * Checks if the table or view with the specified name exists in the specified database.
   */
  override def tableExists(dbName: String, tableName: String): Boolean = {
    val tableIdent = TableIdentifier(tableName, Option(dbName))
    sessionCatalog.isTempView(tableIdent) || sessionCatalog.tableExists(tableIdent)
  }

  /**
   * Checks if the function with the specified name exists. This can either be a temporary function
   * or a function.
   */
  override def functionExists(functionName: String): Boolean = {
    val functionIdent = hbaseSession.sessionState.sqlParser.parseFunctionIdentifier(functionName)
    functionExists(functionIdent.database.orNull, functionIdent.funcName)
  }

  /**
   * Checks if the function with the specified name exists in the specified database.
   */
  override def functionExists(dbName: String, functionName: String): Boolean = {
    sessionCatalog.functionExists(FunctionIdentifier(functionName, Option(dbName)))
  }

  /**
   * :: Experimental ::
   * Creates a table from the given path and returns the corresponding DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
   *
   * @group ddl_ops
   * @since 2.2.0
   */
  @Experimental
  override def createTable(tableName: String, path: String): DataFrame = {
    // 这个方法会创建parquet table
    val dataSourceName = hbaseSession.sessionState.conf.defaultDataSourceName
    createTable(tableName, path, dataSourceName)
  }

  /**
   * :: Experimental ::
   * Creates a table from the given path and returns the corresponding
   * DataFrame.
   *
   * @group ddl_ops
   * @since 2.2.0
   */
  @Experimental
  override def createTable(tableName: String, path: String, source: String): DataFrame = {
    // 根据source创建表, source="hbase"时会创建hbase表
    createTable(tableName, source, Map("path" -> path))
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Creates a table based on the dataset in a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 2.2.0
   */
  @Experimental
  override def createTable(
                            tableName: String,
                            source: String,
                            options: Map[String, String]): DataFrame = {
    createTable(tableName, source, new StructType, options)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Creates a table based on the dataset in a data source, a schema and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 2.2.0
   */
  @Experimental
  override def createTable(
                            tableName: String,
                            source: String,
                            schema: StructType,
                            options: Map[String, String]): DataFrame = {
    createTable(tableName = tableName,
      source = source,
      schema = schema,
      description = "",
      options = options)
  }

  override def createTable(
                            tableName: String,
                            source: String,
                            description: String,
                            options: Map[String, String]): DataFrame = {
    createTable(tableName, source, new StructType, description, options)
  }

  /**
   * (Scala-specific)
   * Creates a table based on the dataset in a data source, a schema and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 3.1.0
   */
  override def createTable(
                            tableName: String,
                            source: String,
                            schema: StructType,
                            description: String,
                            options: Map[String, String]): DataFrame = {
    val tableIdent = hbaseSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    val storage = DataSource.buildStorageFormatFromOptions(options)
    val tableType = if (storage.locationUri.isDefined) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }
    val tableDesc = CatalogTable(
      identifier = tableIdent,
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = Some(source),
      comment = {
        if (description.isEmpty) None else Some(description)
      }
    )
    val plan = CreateTable(tableDesc, SaveMode.ErrorIfExists, None)
    hbaseSession.sessionState.executePlan(plan).toRdd
    hbaseSession.table(tableIdent)
  }


  /**
   * Drops the local temporary view with the given view name in the catalog.
   * If the view has been cached/persisted before, it's also unpersisted.
   *
   * @param viewName the identifier of the temporary view to be dropped.
   * @group ddl_ops
   * @since 2.0.0
   */
  override def dropTempView(viewName: String): Boolean = {
    hbaseSession.sessionState.catalog.dropTempView(viewName)
  }

  /**
   * Drops the global temporary view with the given view name in the catalog.
   * If the view has been cached/persisted before, it's also unpersisted.
   *
   * @param viewName the identifier of the global temporary view to be dropped.
   * @group ddl_ops
   * @since 2.1.0
   */
  override def dropGlobalTempView(viewName: String): Boolean = {
    hbaseSession.sessionState.catalog.dropGlobalTempView(viewName)
  }

  /**
   * Recovers all the partitions in the directory of a table and update the catalog.
   * Only works with a partitioned table, and not a temporary view.
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in the
   *                  current database.
   * @group ddl_ops
   * @since 2.1.1
   */
  override def recoverPartitions(tableName: String): Unit = {
    val tableIdent = hbaseSession.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    hbaseSession.sessionState.executePlan(
      RecoverPartitions(
        UnresolvedTable(tableIdent, "recoverPartitions()", None))).toRdd
  }

  /**
   * Returns true if the table or view is currently cached in-memory.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def isCached(tableName: String): Boolean = {
    hbaseSession.sharedState.cacheManager.lookupCachedData(hbaseSession.table(tableName)).nonEmpty
  }

  /**
   * Caches the specified table or view in-memory.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def cacheTable(tableName: String): Unit = {
    hbaseSession.sharedState.cacheManager.cacheQuery(hbaseSession.table(tableName), Some(tableName))
  }

  /**
   * Caches the specified table or view with the given storage level.
   *
   * @group cachemgmt
   * @since 2.3.0
   */
  override def cacheTable(tableName: String, storageLevel: StorageLevel): Unit = {
    hbaseSession.sharedState.cacheManager.cacheQuery(
      hbaseSession.table(tableName), Some(tableName), storageLevel)
  }

  /**
   * Removes the specified table or view from the in-memory cache.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def uncacheTable(tableName: String): Unit = {
    hbaseSession.catalog.uncacheTable(tableName)
  }

  /**
   * Removes all cached tables or views from the in-memory cache.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def clearCache(): Unit = {
    hbaseSession.sharedState.cacheManager.clearCache()
  }

  /**
   * Returns true if the [[Dataset]] is currently cached in-memory.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  protected[sql] def isCached(qName: Dataset[_]): Boolean = {
    hbaseSession.sharedState.cacheManager.lookupCachedData(qName).nonEmpty
  }

  /**
   * Invalidates and refreshes all the cached data and metadata of the given table or view.
   * For Hive metastore table, the metadata is refreshed. For data source tables, the schema will
   * not be inferred and refreshed.
   *
   * If this table is cached as an InMemoryRelation, drop the original cached version and make the
   * new version cached lazily.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def refreshTable(tableName: String): Unit = {
    val tableIdent = hbaseSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    // Temp tables: refresh (or invalidate) any metadata/data cached in the plan recursively.
    // Non-temp tables: refresh the metadata cache.
    sessionCatalog.refreshTable(tableIdent)

    // If this table is cached as an InMemoryRelation, drop the original
    // cached version and make the new version cached lazily.
    val table = hbaseSession.table(tableIdent)
    if (isCached(table)) {
      // Uncache the logicalPlan.
      hbaseSession.sharedState.cacheManager.uncacheQuery(table, cascade = true)
      // Cache it again.
      hbaseSession.sharedState.cacheManager.cacheQuery(table, Some(tableIdent.table))
    }
  }

  /**
   * Refreshes the cache entry and the associated metadata for all Dataset (if any), that contain
   * the given data source path. Path matching is by prefix, i.e. "/" would invalidate
   * everything that is cached.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def refreshByPath(resourcePath: String): Unit = {
    hbaseSession.sharedState.cacheManager.recacheByPath(hbaseSession, resourcePath)
  }

  override def listNamespaces(): Array[Array[String]] = {
    Array(sessionCatalog.listDatabases().toArray)
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = namespace match {
    case Array(db) if sessionCatalog.databaseExists(db) =>
      Array(sessionCatalog.listDatabases(db).toArray)

    case Array(_) =>
      throw QueryCompilationErrors.noSuchNamespaceError(namespace)

    case _ =>
      throw QueryExecutionErrors.invalidNamespaceNameError(namespace)
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = namespace match {
    case Array(db) if sessionCatalog.databaseExists(db) =>
      sessionCatalog.getDatabaseMetadata(db).properties.asJava

    case Array(_) =>
      throw QueryCompilationErrors.noSuchNamespaceError(namespace)

    case _ =>
      throw QueryExecutionErrors.invalidNamespaceNameError(namespace)
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = namespace match {
    case Array(db) if !sessionCatalog.databaseExists(db) =>
      sessionCatalog.createDatabase(CatalogDatabase(db, db + "description",
        new URI(hbaseSession.config.get("hbase.rootdit") + s"/data/$db"),
        metadata.asScala.toMap), ignoreIfExists = true)

    case Array(_) =>
      throw QueryCompilationErrors.namespaceAlreadyExistsError(namespace)

    case _ =>
      throw QueryExecutionErrors.invalidNamespaceNameError(namespace)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = namespace match {
    case Array(db) if sessionCatalog.databaseExists(db) =>
      sessionCatalog.alterDatabase(CatalogDatabase(db, db + "description",
        new URI(hbaseSession.config.get("hbase.rootdit") + s"/data/$db"), Map.empty))

    case Array(_) =>
      throw QueryCompilationErrors.namespaceAlreadyExistsError(namespace)

    case _ =>
      throw QueryExecutionErrors.invalidNamespaceNameError(namespace)
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = namespace match {
    case Array(db) if sessionCatalog.databaseExists(db) =>
      sessionCatalog.dropDatabase(db, ignoreIfNotExists = true, cascade = true)
      true

    case Array(_) =>
      throw QueryCompilationErrors.noSuchNamespaceError(namespace)

    case _ =>
      throw QueryExecutionErrors.invalidNamespaceNameError(namespace)
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
  }

  override def name(): String = "hbase_catalog"

  override def currentCatalog(): String =
    hbaseSession.sessionState.catalogManager.currentCatalog.name()

  override def setCurrentCatalog(catalogName: String): Unit =
    hbaseSession.sessionState.catalogManager.setCurrentCatalog(catalogName)

  override def listCatalogs(): Dataset[CatalogMetadata] = {
    val catalogs = hbaseSession.sessionState.catalogManager.listCatalogs(None)
    HBaseCatalogImpl.makeDataset(catalogs.map(name => makeCatalog(name)), hbaseSession)
  }

  private def makeCatalog(name: String): CatalogMetadata = {
    new CatalogMetadata(
      name = name,
      description = null)
  }

  override def listDatabases(pattern: String): Dataset[Database] = {
    listDatabases().filter(_.name.matches(pattern))
  }

  override def listTables(dbName: String, pattern: String): Dataset[Table] = {
    listTables(dbName).filter(_.name.matches(pattern))
  }

  override def listFunctions(dbName: String, pattern: String): Dataset[Function] = {
    listFunctions(dbName).filter(_.name.matches(pattern))
  }

  override def listCatalogs(pattern: String): Dataset[CatalogMetadata] = {
    listCatalogs().filter(_.name.matches(pattern))
  }
}

private[sql] object HBaseCatalogImpl {

  def makeDataset[T <: DefinedByConstructorParams : TypeTag](
                                                              data: Seq[T],
                                                              sparkSession: SparkSession): Dataset[T] = {
    val enc = ExpressionEncoder[T]()
    val serializer = enc.createSerializer()
    val encoded = data.map(d => serializer(d).copy())

    val plan = new LocalRelation(enc.schema.toAttributes, encoded)
    val queryExecution = sparkSession.sessionState.executePlan(plan)
    new Dataset[T](queryExecution, enc)
  }

}
