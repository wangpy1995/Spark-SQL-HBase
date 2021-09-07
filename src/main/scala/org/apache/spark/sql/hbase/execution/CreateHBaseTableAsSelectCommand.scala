package org.apache.spark.sql.hbase.execution

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}

import scala.util.control.NonFatal

/**
 * Create table and insert the query result into it.
 *
 * @param tableDesc the Table Describe, which may contains serde, storage handler etc.
 * @param query     the query whose result will be insert into the new relation
 * @param mode      SaveMode
 */
case class CreateHBaseTableAsSelectCommand(
                                            tableDesc: CatalogTable,
                                            query: LogicalPlan,
                                            mode: SaveMode)
  extends RunnableCommand {

  private val tableIdentifier = tableDesc.identifier

  override def innerChildren: Seq[LogicalPlan] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (sparkSession.sessionState.catalog.tableExists(tableIdentifier)) {
      assert(mode != SaveMode.Overwrite,
        s"Expect the table $tableIdentifier has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw new AnalysisException(s"$tableIdentifier already exists.")
      }
      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        return Seq.empty
      }

      sparkSession.sessionState.executePlan(
        InsertIntoStatement(
          UnresolvedRelation(tableIdentifier),
          Map(),
          Seq.empty,
          query,
          overwrite = false,
          ifPartitionNotExists = false)).toRdd
    } else {
      // TODO ideally, we should get the output data ready first and then
      // add the relation into catalog, just in case of failure occurs while data
      // processing.
      assert(tableDesc.schema.isEmpty)
      sparkSession.sessionState.catalog.createTable(
        tableDesc.copy(schema = query.schema), ignoreIfExists = false)

      try {
        sparkSession.sessionState.executePlan(
          InsertIntoStatement(
            UnresolvedRelation(tableIdentifier),
            Map(),
            Seq.empty,
            query,
            overwrite = true,
            ifPartitionNotExists = false)).toRdd
      } catch {
        case NonFatal(e) =>
          // drop the created table.
          sparkSession.sessionState.catalog.dropTable(tableIdentifier, ignoreIfNotExists = true,
            purge = false)
          throw e
      }
    }

    Seq.empty[Row]
  }

  def argString: String = {
    s"[Database:${tableDesc.database}}, " +
      s"TableName: ${tableDesc.identifier.table}, " +
      s"InsertIntoHBaseTable]"
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    assert(newChildren.size == 1, "Incorrect number of children")
    copy(query=newChildren.head)
  }
}
