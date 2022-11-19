package org.apache.spark.sql.hbase.execution.datasources

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.{CreateTable => CreateTableV1}
import org.apache.spark.sql.hbase.HBaseAnalysis

object HBaseOnlyCheck extends (LogicalPlan => Unit) {
  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case CreateTableV1(tableDesc, _, _) if HBaseAnalysis.isHBaseTable(tableDesc) =>
        throw QueryCompilationErrors.ddlWithoutHiveSupportEnabledError(
          "CREATE Hbase TABLE (AS SELECT)")
      case _ => // OK
    }
  }
}