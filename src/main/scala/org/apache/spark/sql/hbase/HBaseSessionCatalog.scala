package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, GetViewColumnByNameAndOrdinal, NoSuchTableException, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Alias, UpCast}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias, View}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.Locale

/**
  * Created by wpy on 17-5-17.
  */
private[sql] class HBaseSessionCatalog(
                                        externalCatalogBuilder: () => ExternalCatalog,
                                        globalTempViewManagerBuilder: () => GlobalTempViewManager,
                                        functionRegistry: FunctionRegistry,
                                        tableFunctionRegistry: TableFunctionRegistry,
                                        hadoopConf: Configuration,
                                        parser: ParserInterface,
                                        functionResourceLoader: FunctionResourceLoader)
  extends SessionCatalog(
    externalCatalogBuilder,
    globalTempViewManagerBuilder,
    functionRegistry,
    tableFunctionRegistry,
    hadoopConf,
    parser,
    functionResourceLoader) {


  override def refreshTable(name: TableIdentifier): Unit = {
    super.refreshTable(name)
  }

  override def lookupRelation(name: TableIdentifier): LogicalPlan = {
    synchronized {
      val db = formatDatabaseName(name.database.getOrElse(currentDb))
      val table = formatTableName(name.table)
      if (db == globalTempViewManager.database) {
        globalTempViewManager.get(table).map { viewDef =>
          SubqueryAlias(table, viewDef)
        }.getOrElse(throw new NoSuchTableException(db, table))
      } else if (name.database.isDefined || !tempViews.contains(table)) {
        val metadata = externalCatalog.getTable(db, table)
        if (metadata.tableType == CatalogTableType.VIEW) {
          val viewText = metadata.viewText.getOrElse(sys.error("Invalid view without text."))
          // The relation is a view, so we wrap the relation by:
          // 1. Add a [[View]] operator over the relation to keep track of the view desc;
          // 2. Wrap the logical plan in a [[SubqueryAlias]] which tracks the name of the view.
          val child = View(
            desc = metadata,
            isTempView = true,
            child = parser.parsePlan(viewText))
          SubqueryAlias(table, child)
        } else {
          getRelation(metadata)
        }
      } else {
        SubqueryAlias(table, getTempViewPlan(tempViews(table)))
      }
    }
  }

  override def getRelation(metadata: CatalogTable, options: CaseInsensitiveStringMap): LogicalPlan = {
    val name = metadata.identifier
    val db = formatDatabaseName(name.database.getOrElse(currentDb))
    val table = formatTableName(name.table)
    val multiParts = Seq(CatalogManager.SESSION_CATALOG_NAME, db, table)

    if (metadata.tableType == CatalogTableType.VIEW) {
      // The relation is a view, so we wrap the relation by:
      // 1. Add a [[View]] operator over the relation to keep track of the view desc;
      // 2. Wrap the logical plan in a [[SubqueryAlias]] which tracks the name of the view.
      SubqueryAlias(multiParts, fromCatalogTable(metadata, isTempView = false))
    } else {
      val tablePlan = HBasePlan(
        metadata,
        // we assume all the columns are nullable.
        metadata.schema.toAttributes,
        metadata.dataSchema.asNullable.toAttributes,
        metadata.partitionSchema.asNullable.toAttributes)
      SubqueryAlias(table, tablePlan)
    }
  }

  private def getTempViewPlan(viewInfo: TemporaryViewRelation): View = viewInfo.plan match {
    case Some(p) => View(desc = viewInfo.tableMeta, isTempView = true, child = p)
    case None => fromCatalogTable(viewInfo.tableMeta, isTempView = true)
  }

  private def fromCatalogTable(metadata: CatalogTable, isTempView: Boolean): View = {
    val viewText = metadata.viewText.getOrElse {
      throw new IllegalStateException("Invalid view without text.")
    }
    val viewConfigs = metadata.viewSQLConfigs
    val parsedPlan = SQLConf.withExistingConf(View.effectiveSQLConf(viewConfigs, isTempView)) {
      parser.parsePlan(viewText)
    }
    val viewColumnNames = if (metadata.viewQueryColumnNames.isEmpty) {
      // For view created before Spark 2.2.0, the view text is already fully qualified, the plan
      // output is the same with the view output.
      metadata.schema.fieldNames.toSeq
    } else {
      assert(metadata.viewQueryColumnNames.length == metadata.schema.length)
      metadata.viewQueryColumnNames
    }

    // For view queries like `SELECT * FROM t`, the schema of the referenced table/view may
    // change after the view has been created. We need to add an extra SELECT to pick the columns
    // according to the recorded column names (to get the correct view column ordering and omit
    // the extra columns that we don't require), with UpCast (to make sure the type change is
    // safe) and Alias (to respect user-specified view column names) according to the view schema
    // in the catalog.
    // Note that, the column names may have duplication, e.g. `CREATE VIEW v(x, y) AS
    // SELECT 1 col, 2 col`. We need to make sure that the matching attributes have the same
    // number of duplications, and pick the corresponding attribute by ordinal.
    val viewConf = View.effectiveSQLConf(metadata.viewSQLConfigs, isTempView)
    val normalizeColName: String => String = if (viewConf.caseSensitiveAnalysis) {
      identity
    } else {
      _.toLowerCase(Locale.ROOT)
    }
    val nameToCounts = viewColumnNames.groupBy(normalizeColName).mapValues(_.length)
    val nameToCurrentOrdinal = scala.collection.mutable.HashMap.empty[String, Int]
    val viewDDL = buildViewDDL(metadata, isTempView)

    val projectList = viewColumnNames.zip(metadata.schema).map { case (name, field) =>
      val normalizedName = normalizeColName(name)
      val count = nameToCounts(normalizedName)
      val ordinal = nameToCurrentOrdinal.getOrElse(normalizedName, 0)
      nameToCurrentOrdinal(normalizedName) = ordinal + 1
      val col = GetViewColumnByNameAndOrdinal(
        metadata.identifier.toString, name, ordinal, count, viewDDL)
      Alias(UpCast(col, field.dataType), field.name)(explicitMetadata = Some(field.metadata))
    }
    View(desc = metadata, isTempView = isTempView, child = Project(projectList, parsedPlan))
  }

  private def buildViewDDL(metadata: CatalogTable, isTempView: Boolean): Option[String] = {
    if (isTempView) {
      None
    } else {
      val viewName = metadata.identifier.unquotedString
      val viewText = metadata.viewText.get
      val userSpecifiedColumns =
        if (metadata.schema.fieldNames.toSeq == metadata.viewQueryColumnNames) {
          ""
        } else {
          s"(${metadata.schema.fieldNames.mkString(", ")})"
        }
      Some(s"CREATE OR REPLACE VIEW $viewName $userSpecifiedColumns AS $viewText")
    }
  }
}
