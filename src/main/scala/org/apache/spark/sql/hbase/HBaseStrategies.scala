package org.apache.spark.sql.hbase

import java.util.Locale

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeReference, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{CreateTableCommand, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy.selectFilters
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation}
import org.apache.spark.sql.hbase.execution.{CreateHBaseTableAsSelectCommand, HBaseTableScanExec, InsertIntoHBaseTable}
import org.apache.spark.sql.sources.{Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wpy on 17-5-17.
  */
private[hbase] trait HBaseStrategies {
  self: SparkPlanner =>

  val sparkSession: SparkSession

  object HBaseDataSource extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case PhysicalOperation(projects, filters, l@LogicalRelation(t: PrunedFilteredScan, _, _, _)) =>
          pruneFilterProject(
            l,
            projects,
            filters,
            (a, f) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f))) :: Nil
        case CreateHBaseTableAsSelectCommand(tableDesc, query, mode) =>
          val cmd = CreateHBaseTableAsSelectCommand(tableDesc, query, mode
          )
          ExecutedCommandExec(cmd) :: Nil
        case _ => Nil
      }
    }

    /**
      * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
      */
    private[this] def toCatalystRDD(
                                     relation: LogicalRelation,
                                     output: Seq[Attribute],
                                     rdd: RDD[Row]): RDD[InternalRow] = {
      if (relation.relation.needConversion) {
        RDDConversions.rowToRowRdd(rdd, output.map(_.dataType))
      } else {
        rdd.asInstanceOf[RDD[InternalRow]]
      }
    }

    // Based on Public API.
    private def pruneFilterProject(
                                    relation: LogicalRelation,
                                    projects: Seq[NamedExpression],
                                    filterPredicates: Seq[Expression],
                                    scanBuilder: (Seq[Attribute], Array[Filter]) => RDD[InternalRow]) = {
      pruneFilterProjectRaw(
        relation,
        projects,
        filterPredicates,
        (requestedColumns, _, pushedFilters) => {
          scanBuilder(requestedColumns, pushedFilters.toArray)
        })
    }

    private def pruneFilterProjectRaw(
                                       relation: LogicalRelation,
                                       projects: Seq[NamedExpression],
                                       filterPredicates: Seq[Expression],
                                       scanBuilder: (Seq[Attribute], Seq[Expression], Seq[Filter]) => RDD[InternalRow]): SparkPlan = {

      val projectSet = AttributeSet(projects.flatMap(_.references))
      val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

      val candidatePredicates = filterPredicates.map {
        _ transform {
          case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
        }
      }

      val (unhandledPredicates, pushedFilters, handledFilters) =
        selectFilters(relation.relation, candidatePredicates)

      // A set of column attributes that are only referenced by pushed down filters.  We can eliminate
      // them from requested columns.
      val handledSet = {
        val handledPredicates = filterPredicates.filterNot(unhandledPredicates.contains)
        val unhandledSet = AttributeSet(unhandledPredicates.flatMap(_.references))
        AttributeSet(handledPredicates.flatMap(_.references)) --
          (projectSet ++ unhandledSet).map(relation.attributeMap)
      }

      // Combines all Catalyst filter `Expression`s that are either not convertible to data source
      // `Filter`s or cannot be handled by `relation`.
      val filterCondition = unhandledPredicates.reduceLeftOption(expressions.And)

      // These metadata values make scan plans uniquely identifiable for equality checking.
      // TODO(SPARK-17701) using strings for equality checking is brittle
      val metadata: Map[String, String] = {
        val pairs = ArrayBuffer.empty[(String, String)]

        // Mark filters which are handled by the underlying DataSource with an Astrisk
        if (pushedFilters.nonEmpty) {
          val markedFilters = for (filter <- pushedFilters) yield {
            if (handledFilters.contains(filter)) s"*$filter" else s"$filter"
          }
          pairs += ("PushedFilters" -> markedFilters.mkString("[", ", ", "]"))
        }
        pairs += ("ReadSchema" ->
          StructType.fromAttributes(projects.map(_.toAttribute)).catalogString)
        pairs.toMap
      }

      if (projects.map(_.toAttribute) == projects &&
        projectSet.size == projects.size &&
        filterSet.subsetOf(projectSet)) {
        // When it is possible to just use column pruning to get the right projection and
        // when the columns of this projection are enough to evaluate all filter conditions,
        // just do a scan followed by a filter, with no extra project.
        val requestedColumns = projects
          // Safe due to if above.
          .asInstanceOf[Seq[Attribute]]
          // Match original case of attributes.
          .map(relation.attributeMap)
          // Don't request columns that are only referenced by pushed filters.
          .filterNot(handledSet.contains)

        val scan = RowDataSourceScanExec(
          projects.map(_.toAttribute),
          projects.map(_.toAttribute).indices,
          pushedFilters.toSet,
          pushedFilters.toSet,
          scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
          relation.relation,
          relation.catalogTable.map(_.identifier))
        filterCondition.map(FilterExec(_, scan)).getOrElse(scan)
      } else {
        // Don't request columns that are only referenced by pushed filters.
        val requestedColumns =
          (projectSet ++ filterSet -- handledSet).map(relation.attributeMap).toSeq

        val scan = RowDataSourceScanExec(
          requestedColumns,
          requestedColumns.indices,
          pushedFilters.toSet,
          pushedFilters.toSet,
          scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
          relation.relation,
          relation.catalogTable.map(_.identifier))
        ProjectExec(
          projects, filterCondition.map(FilterExec(_, scan)).getOrElse(scan))
      }
    }
  }

  object HBaseTableScans extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filter, relation: HBaseRelation) =>
        /*  pruneFilterProject(
            projectList,
            filter,
            identity[Seq[Expression]],
            HBaseTableScanExec(_, relation, filter)(sparkSession.asInstanceOf[HBaseSession])) :: Nil*/
        filterProject4HBase(relation, projectList, filter) :: Nil
      case _ =>
        Nil
    }
  }

  protected def filterProject4HBase(relation: HBaseRelation, projectList: Seq[NamedExpression], filterPredicates: Seq[Expression]): SparkPlan = {
    val attributeMap: AttributeMap[AttributeReference] = AttributeMap(relation.output.map(o => (o, o)))
    val projectSet = AttributeSet(projectList.flatMap(_.references))

    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filters = if (filterPredicates.nonEmpty) {
      Seq(
        filterPredicates.map {
          _ transform { case a: AttributeReference => attributeMap(a) }
        }.reduceLeft(And)
      )
    }
    else filterPredicates
    if (projectList.map(_.toAttribute) == projectList && projectSet.size == projectList.size && filterSet.subsetOf(projectSet)) {
      val requestedColumns = projectList.asInstanceOf[Seq[Attribute]].map(attributeMap)
      HBaseTableScanExec(requestedColumns, relation, filters)(sparkSession.asInstanceOf[HBaseSession])
    }
    else {
      //val requestedColumns = projectSet.map(relation.attributeMap ).toSeq
      val requestedColumns = attributeMap.keySet.toSeq
      val scan = HBaseTableScanExec(requestedColumns, relation, filters)(sparkSession.asInstanceOf[HBaseSession])
      ProjectExec(projectList, scan)
    }
  }
}

/**
  * Replaces generic operations with specific variants that are designed to work with Hive.
  *
  * Note that, this rule must be run after `PreprocessTableCreation` and
  * `PreprocessTableInsertion`.
  */
object HBaseAnalysis extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp  {
    case InsertIntoTable(relation: HBaseRelation, _, query, overwrite, ifNotExists)
      if isHBaseTable(relation.tableMeta) =>
      InsertIntoHBaseTable(relation.tableMeta, query, overwrite, ifNotExists)

    case CreateTable(tableDesc, mode, None) if isHBaseTable(tableDesc) =>
      CreateTableCommand(tableDesc, ignoreIfExists = mode == SaveMode.Ignore)

    case CreateTable(tableDesc, mode, Some(query)) if isHBaseTable(tableDesc) =>
      CreateHBaseTableAsSelectCommand(tableDesc, query, mode)
  }

  def isHBaseTable(table: CatalogTable): Boolean = {
    table.provider.isDefined && table.provider.get.toLowerCase(Locale.ROOT) == "hbase"
  }
}