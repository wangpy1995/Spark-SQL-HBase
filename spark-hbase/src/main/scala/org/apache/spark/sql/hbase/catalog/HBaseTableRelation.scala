package org.apache.spark.sql.hbase.catalog

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf

case class HBaseTableRelation(
                               tableMeta: CatalogTable,
                               dataCols: Seq[AttributeReference],
                               partitionCols: Seq[AttributeReference],
                               tableStats: Option[Statistics] = None,
                               @transient prunedPartitions: Option[Seq[CatalogTablePartition]] = None)
  extends LeafNode with MultiInstanceRelation {
  assert(tableMeta.identifier.database.isDefined)
  assert(tableMeta.partitionSchema.sameType(partitionCols.toStructType))
  assert(tableMeta.dataSchema.sameType(dataCols.toStructType))

  // The partition column should always appear after data columns.
  override def output: Seq[AttributeReference] = dataCols ++ partitionCols

  def isPartitioned: Boolean = partitionCols.nonEmpty

  override def doCanonicalize(): HBaseTableRelation = copy(
    tableMeta = CatalogTable.normalize(tableMeta),
    dataCols = dataCols.zipWithIndex.map {
      case (attr, index) => attr.withExprId(ExprId(index))
    },
    partitionCols = partitionCols.zipWithIndex.map {
      case (attr, index) => attr.withExprId(ExprId(index + dataCols.length))
    },
    tableStats = None
  )

  override def computeStats(): Statistics = {
    tableMeta.stats.map(_.toPlanStats(output, conf.cboEnabled || conf.planStatsEnabled))
      .orElse(tableStats)
      .getOrElse {
        throw new IllegalStateException("Table stats must be specified.")
      }
  }

  override def newInstance(): HBaseTableRelation = copy(
    dataCols = dataCols.map(_.newInstance()),
    partitionCols = partitionCols.map(_.newInstance()))

  override def simpleString(maxFields: Int): String = {
    val catalogTable = tableMeta.storage.serde match {
      case Some(serde) => tableMeta.identifier :: serde :: Nil
      case _ => tableMeta.identifier :: Nil
    }

    var metadata = Map(
      "CatalogTable" -> catalogTable.mkString(", "),
      "Data Cols" -> truncatedString(dataCols, "[", ", ", "]", maxFields),
      "Partition Cols" -> truncatedString(partitionCols, "[", ", ", "]", maxFields)
    )

    if (prunedPartitions.nonEmpty) {
      metadata += ("Pruned Partitions" -> {
        val parts = prunedPartitions.get.map { part =>
          val spec = part.spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
          if (part.storage.serde.nonEmpty && part.storage.serde != tableMeta.storage.serde) {
            s"($spec, ${part.storage.serde.get})"
          } else {
            s"($spec)"
          }
        }
        truncatedString(parts, "[", ", ", "]", maxFields)
      })
    }

    val metadataEntries = metadata.toSeq.map {
      case (key, value) if key == "CatalogTable" => value
      case (key, value) =>
        key + ": " + StringUtils.abbreviate(value, SQLConf.get.maxMetadataStringLength)
    }

    val metadataStr = truncatedString(metadataEntries, "[", ", ", "]", maxFields)
    s"$nodeName $metadataStr"
  }
}
