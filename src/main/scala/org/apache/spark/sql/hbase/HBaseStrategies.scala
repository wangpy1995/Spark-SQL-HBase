package org.apache.spark.sql.hbase

import java.util.Locale
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeReference, AttributeSet, Expression, GenericInternalRow, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, QualifiedTableName, expressions}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{CreateTableCommand, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy.selectFilters
import org.apache.spark.sql.execution.datasources.v2.PushedDownOperators
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource, DataSourceUtils, InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hbase.catalog.HBaseTableRelation
import org.apache.spark.sql.hbase.execution.{CreateHBaseTableAsSelectCommand, HBaseFileFormat, HBaseTableScanExec, InsertIntoHBaseTable}
import org.apache.spark.sql.sources.{Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.mutable.ArrayBuffer

/**
 * Created by wpy on 17-5-17.
 */
private[hbase] trait HBaseStrategies {
  self: SparkPlanner =>

  val sparkSession: SparkSession

  object HBaseDataSource extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projects, filters, l@LogicalRelation(t: PrunedFilteredScan, _, _, _))
        if l.catalogTable.isDefined && HBaseAnalysis.isHBaseTable(l.catalogTable.get) =>
        pruneFilterProject(
          l,
          projects,
          filters,
          (a, f) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f))) :: Nil
      case CreateHBaseTableAsSelectCommand(tableDesc, query, mode)
        if HBaseAnalysis.isHBaseTable(tableDesc) =>
        val cmd = CreateHBaseTableAsSelectCommand(tableDesc, query, mode)
        ExecutedCommandExec(cmd) :: Nil
      case _ => Nil
    }

    /**
     * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
     */
    private[this] def toCatalystRDD(
                                     relation: LogicalRelation,
                                     output: Seq[Attribute],
                                     rdd: RDD[Row]): RDD[InternalRow] = {
      if (relation.relation.needConversion) {
        rowToRowRdd(rdd, output.map(_.dataType))
      } else {
        rdd.asInstanceOf[RDD[InternalRow]]
      }
    }

    /**
     * Convert the objects inside Row into the types Catalyst expected.
     */
    private def rowToRowRdd(data: RDD[Row], outputTypes: Seq[DataType]): RDD[InternalRow] = {
      data.mapPartitions { iterator =>
        val numColumns = outputTypes.length
        val mutableRow = new GenericInternalRow(numColumns)
        val converters = outputTypes.map(CatalystTypeConverters.createToCatalystConverter)
        iterator.map { r =>
          var i = 0
          while (i < numColumns) {
            mutableRow(i) = converters(i)(r(i))
            i += 1
          }

          mutableRow
        }
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
          StructType.fromAttributes(projects.map(_.toAttribute)),
          pushedFilters.toSet,
          pushedFilters.toSet,
          PushedDownOperators(None, None, None, None, Seq.empty, Seq.empty),
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
          StructType.fromAttributes(requestedColumns),
          pushedFilters.toSet,
          pushedFilters.toSet,
          PushedDownOperators(None, None, None, None, Seq.empty, Seq.empty),
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
      case PhysicalOperation(projectList, filter, h@HBaseTableRelation(tableMeta, dataCols, _, _, _))
        if HBaseAnalysis.isHBaseTable(tableMeta) =>
        val plan = HBasePlan(tableMeta, dataCols, h.output, h.partitionCols)
        filterProject4HBase(plan, projectList, filter) :: Nil
      case PhysicalOperation(projectList, filter, plan: HBasePlan)
        if HBaseAnalysis.isHBaseTable(plan.tableMeta) =>
        /*  pruneFilterProject(
            projectList,
            filter,
            identity[Seq[Expression]],
            HBaseTableScanExec(_, relation, filter)(sparkSession.asInstanceOf[HBaseSession])) :: Nil*/
        filterProject4HBase(plan, projectList, filter) :: Nil
      case _ =>
        Nil
    }
  }

  protected def filterProject4HBase(plan: HBasePlan, projectList: Seq[NamedExpression], filterPredicates: Seq[Expression]): SparkPlan = {
    val attributeMap: AttributeMap[AttributeReference] = AttributeMap(plan.output.map(o => (o, o)))
    val projectSet = AttributeSet(projectList.flatMap(_.references))

    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filters = if (filterPredicates.nonEmpty) {
      Seq(
        filterPredicates.map {
          _ transform { case a: AttributeReference => attributeMap(a) }
        }.reduceLeft(And)
      )
    } else {
      filterPredicates
    }
    if (projectList.map(_.toAttribute) == projectList && projectSet.size == projectList.size && filterSet.subsetOf(projectSet)) {
      val requestedColumns = projectList.asInstanceOf[Seq[Attribute]].map(attributeMap)
      HBaseTableScanExec(requestedColumns, plan, filters)(sparkSession.asInstanceOf[HBaseSession])
    } else {
      val requestedColumns = projectSet.toSeq
      //      val requestedColumns = attributeMap.keySet.toSeq
      val scan = HBaseTableScanExec(requestedColumns, plan, filters)(sparkSession.asInstanceOf[HBaseSession])
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
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case InsertIntoStatement(plan: HBasePlan, _, _, query, overwrite, ifNotExists)
      if isHBaseTable(plan.tableMeta) =>
      InsertIntoHBaseTable(plan.tableMeta, query, overwrite, ifNotExists)

    case CreateTable(tableDesc, mode, None) if isHBaseTable(tableDesc) =>
      CreateTableCommand(tableDesc, ignoreIfExists = mode == SaveMode.Ignore)

    case CreateTable(tableDesc, mode, Some(query)) if isHBaseTable(tableDesc) =>
      CreateHBaseTableAsSelectCommand(tableDesc, query, mode)
  }

  def isHBaseTable(table: CatalogTable): Boolean = {
    table.provider.isDefined && table.provider.get.toLowerCase(Locale.ROOT) == "hbase"
  }
}

class ResolveHBaseTable(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  def readHBaseTable(table: CatalogTable, extraOptions: CaseInsensitiveStringMap): LogicalPlan = {
    HBasePlan(
      table,
      table.dataSchema.asNullable.toAttributes,
      table.dataSchema.asNullable.toAttributes,
      table.partitionSchema.asNullable.toAttributes)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case UnresolvedCatalogRelation(tableMeta, options, false)
      if HBaseAnalysis.isHBaseTable(tableMeta) =>
      readHBaseTable(tableMeta, options)
  }
}