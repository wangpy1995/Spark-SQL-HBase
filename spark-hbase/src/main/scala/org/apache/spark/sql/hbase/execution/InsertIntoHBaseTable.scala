package org.apache.spark.sql.hbase.execution

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hbase.SparkHBaseConstants.TABLE_CONSTANTS
import org.apache.spark.sql.hbase.utils.{HBaseSparkDataUtils, HBaseSparkFormatUtils}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.Utils


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

  private def findRowKeyGenerator(table: CatalogTable): RowKeyGenerator = {
    val generatorClsName = table.properties("generator")
    val classLoader = Utils.getContextOrSparkClassLoader

    val constructor = classLoader.loadClass(generatorClsName).getConstructor()
    constructor.newInstance() match {
      case generator: RowKeyGenerator => generator
      case _ => null
    }
  }

  override def innerChildren: Seq[LogicalPlan] = query :: Nil

  /**
   * Inserts all the rows in the table into HBase.  Row objects are properly serialized with the
   * `org.apache.hadoop.mapred.OutputFormat` provided by the table definition.
   */
  override def run(sparkSession: SparkSession): Seq[Row] = {

    //put data into hbase here
    //反射表schema yml文件中自定义的rowKey生成器
    val generator = findRowKeyGenerator(table)
    //插入语句中是否指定了列名
    val querySchema = if (query.schema.length == table.schema.length) {
      table.schema
    } else {
      query.schema
    }
    //将查询时输入的列名转换为hbase的cf和qualifier的Bytes形式
    val bytesColNames = querySchema.fieldNames
      .map(HBaseSparkFormatUtils.splitColumnAndQualifierName)
      .map { separatedName =>
        Bytes.toBytes(separatedName.familyName) -> Bytes.toBytes(separatedName.qualifierName)
      }
    //尝试获取rowKey字段在插入语句中的位置
    val rowKeyIdx: Int = querySchema.getFieldIndex(TABLE_CONSTANTS.ROW_KEY.getValue).getOrElse(-1)
    val queryContainsRowKey = rowKeyIdx != -1
    //生成对应的转换方法.用于将插入语句中的数据转换为HBase存储时的Bytes
    val rowConverters = HBaseSparkDataUtils.genRowConverters(querySchema)
    Dataset.ofRows(sparkSession, query).rdd.foreachPartition { rowIterator =>
      val conn = ConnectionFactory.createConnection()
      val tableConn = conn.getTable(TableName.valueOf(table.identifier.database.get + ":" + table.identifier.table))
      val putList = new java.util.ArrayList[Put]()
      rowIterator.foreach { row =>
        //处理rowKey
        val rowKey = generator.genRowKey(row)
        val put = new Put(rowKey)
        if (!queryContainsRowKey) {
          for (i <- bytesColNames.indices) {
            val colName = bytesColNames(i)
            put.addColumn(colName._1, colName._2, rowConverters(i)(row, i))
          }
        } else {
          for (i <- bytesColNames.indices) {
            if (i != rowKeyIdx) {
              val colName = bytesColNames(i)
              put.addColumn(colName._1, colName._2, rowConverters(i)(row, i))
            }
          }
        }
        putList.add(put)
        //TODO 改到配置文件里
        if (putList.size() == 1000) {
          tableConn.put(putList)
          putList.clear()
        }
      }
      tableConn.put(putList)
      tableConn.close()
    }
    //TODO here need some tests and should load at last
    // un-cache this table.
//    sparkSession.catalog.uncacheTable(table.identifier.quotedString)
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
