package org.apache.spark.sql.hbase

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LogicalPlanIntegrity}

/**
 * Created by wpy on 17-5-26.
 */
case class HBasePlan(
                      tableMeta: CatalogTable,
                      dataCols: Seq[AttributeReference],
                      output: Seq[AttributeReference],
                      partitionCols: Seq[AttributeReference]
                    ) extends LogicalPlan {
  override def children = Nil

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    assert(newChildren.size == 1, "Incorrect number of children")
    this
  }
}