package org.apache.spark.sql.hbase.execution

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeSet, Expression, UnsafeProjection}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.hbase.utils.{HBaseSparkDataUtils, HBaseSparkFilterUtils}

/**
 * Created by wpy on 17-5-16.
 */
private[hbase]
case class HBaseTableScanExec(
                               requestedAttributes: Seq[Attribute],
                               plan: HBasePlan,
                               filter: Seq[Expression])(
                               @transient private val hbaseSession: HBaseSession)
  extends LeafExecNode {
  val meta = plan.tableMeta
  val parameters = meta.properties
  val tableName = meta.identifier.database.get + ":" + meta.identifier.table

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def producedAttributes: AttributeSet = outputSet ++
    AttributeSet(filter.flatMap(_.references))

  private val originalAttributes = AttributeMap(plan.output.map(a => a -> a))

  override val output: Seq[Attribute] = {
    // Retrieve the original attributes based on expression ID so that capitalization matches.
    requestedAttributes.map(originalAttributes)
  }


  override protected def doExecute(): RDD[InternalRow] = {
    // show num results  in spark web ui
    val numOutputRows = longMetric("numOutputRows")

    val filterLists = filter.map(f => HBaseSparkFilterUtils.buildHBaseFilterList4Where(plan.dataCols, requestedAttributes, Some(f)))
    val hbaseFilter = HBaseSparkFilterUtils.combineHBaseFilterLists(filterLists)

    val scan = new Scan()
    addColumnFamiliesToScan(scan, hbaseFilter, filter)

    //read data from hbase
    hbaseSession.sqlContext.hbaseRDD(TableName.valueOf(tableName), scan)
      .mapPartitionsWithIndexInternal { (index, iter) =>
        val proj = UnsafeProjection.create(schema)
        val columnFamily = schema.map { field =>
          val cf_q = field.name.split("_", 2)
          (Bytes.toBytes(cf_q.head),
            Bytes.toBytes(cf_q.last),
            HBaseSparkDataUtils.genHBaseFieldConverter(field.dataType))
        }
        proj.initialize(index)
        val size = schema.length
        iter.map { result =>
          val r = HBaseSparkDataUtils.hbaseResult2InternalRow(result._2, size, columnFamily)
          numOutputRows += 1
          proj(r)
        }
      }
  }


  override def otherCopyArgs: Seq[AnyRef] = Seq(hbaseSession)

  //columnFamily_QualifierName <=== requestAttribute
  def addColumnFamiliesToScan(scan: Scan, filters: Option[Filter], predicate: Seq[Expression]): Scan = {
    // 添加需要显示结果的列
    requestedAttributes.foreach { qualifier =>
      val column_qualifier = qualifier.name.split("_", 2)
      if (qualifier.name != "row_key")
        scan.addColumn(Bytes.toBytes(column_qualifier.head), Bytes.toBytes(column_qualifier.last))
    }
    // 添加需要过滤的列
    predicate.foreach { qualifier =>
      val column_qualifiers = qualifier.references.map(_.toAttribute).toSet
      column_qualifiers.foreach { qualifier =>
        val column_qualifier = qualifier.name.split("_", 2)
        if (qualifier.name != "row_key")
          scan.addColumn(Bytes.toBytes(column_qualifier.head), Bytes.toBytes(column_qualifier.last))
      }
    }
    //
    scan.setCaching(1000)
    scan.readAllVersions()
    if (filters.isDefined) {
      scan.setFilter(filters.get)
    }
    scan
  }
}
