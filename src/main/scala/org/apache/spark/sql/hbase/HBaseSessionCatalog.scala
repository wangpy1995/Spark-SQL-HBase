package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, View}
import org.apache.spark.sql.internal.SQLConf

/**
  * Created by wpy on 17-5-17.
  */
private[sql] class HBaseSessionCatalog(
                                        externalCatalog: HBaseExternalCatalog,
                                        globalTempViewManager: GlobalTempViewManager,
                                        functionRegistry: FunctionRegistry,
                                        conf: SQLConf,
                                        hadoopConf: Configuration,
                                        parser: ParserInterface,
                                        functionResourceLoader: FunctionResourceLoader)
  extends SessionCatalog(
    externalCatalog,
    globalTempViewManager,
    functionRegistry,
    conf,
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
            output = metadata.schema.toAttributes,
            child = parser.parsePlan(viewText))
          SubqueryAlias(table, child)
        } else {
          val tableRelation = HBaseRelation(
            metadata,
            // we assume all the columns are nullable.
            metadata.schema.toAttributes,
            metadata.dataSchema.asNullable.toAttributes,
            metadata.partitionSchema.asNullable.toAttributes)
          SubqueryAlias(table, tableRelation)
        }
      } else {
        SubqueryAlias(table, tempViews(table))
      }
    }
  }
}
