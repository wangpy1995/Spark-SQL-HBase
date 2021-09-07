package org.apache.spark.sql.hbase.execution

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{Dataset, Row, SparkSession}


/**
  * Command for writing data out to a HBase table.
  *
  * This class is mostly a mess, for legacy reasons (since it evolved in organic ways and had to
  * follow Hive's internal implementations closely, which itself was a mess too). Please don't
  * blame Reynold for this! He was just moving code around!
  *
  * In the future we should converge the write path for Hive with the normal data source write path,
  * as defined in `org.apache.spark.sql.execution.datasources.FileFormatWriter`.
  *
  * @param table       the metadata of the table.
  * @param query       the logical plan representing data to write to.
  * @param overwrite   overwrite existing table or partitions.
  * @param ifNotExists If true, only write if the table or partition does not exist.
  */
case class InsertIntoHBaseTable(
                                 table: CatalogTable,
                                 query: LogicalPlan,
                                 overwrite: Boolean,
                                 ifNotExists: Boolean) extends RunnableCommand {

  override def innerChildren: Seq[LogicalPlan] = query :: Nil

  /**
    * Inserts all the rows in the table into HBase.  Row objects are properly serialized with the
    * `org.apache.hadoop.mapred.OutputFormat` provided by the table definition.
    */
  override def run(sparkSession: SparkSession): Seq[Row] = {

    //put data into hbase here
    val queryExecution = Dataset.ofRows(sparkSession, query).queryExecution
    val conn = ConnectionFactory.createConnection()
    val colNames = queryExecution.optimizedPlan.schema.fieldNames
    val tableConn = conn.getTable(TableName.valueOf(table.identifier.database.get + ":" + table.identifier.table))
    queryExecution.toRdd.foreach { row =>
      val rowKey = Bytes.toBytes(row.hashCode().toString)
      val put = new Put(rowKey)
      for (i <- colNames.indices)
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(colNames(i)), Bytes.toBytes(row.getString(i)))
      tableConn.put(put)
    }
    //TODO here need some tests and should load at last
    // un-cache this table.
    sparkSession.catalog.uncacheTable(table.identifier.quotedString)
    sparkSession.sessionState.catalog.refreshTable(table.identifier)

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hbase compatibility as rules.
    Seq.empty[Row]
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    assert(newChildren.size == 1, "Incorrect number of children")
    copy(query = newChildren.head)
  }
}
