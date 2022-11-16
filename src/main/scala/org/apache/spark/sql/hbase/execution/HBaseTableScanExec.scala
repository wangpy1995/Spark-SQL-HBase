package org.apache.spark.sql.hbase.execution

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeSet, Expression, UnsafeProjection}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.hbase.SparkHBaseConstants.TABLE_CONSTANTS
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.hbase.utils.{HBaseSparkDataUtils, HBaseSparkFilterUtils, HBaseSparkFormatUtils}

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
  val meta: CatalogTable = plan.tableMeta
  val parameters: Map[String, String] = meta.properties
  val tableName: String = meta.identifier.database.get + ":" + meta.identifier.table

  override lazy val metrics: Map[String, SQLMetric] = Map(
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
        //判断schema中是否包含row_key信息
        val zippedSchema = schema.zipWithIndex
        val proj = UnsafeProjection.create(schema)
        val columnFamily = zippedSchema.map { case (field, idx) =>
          val separateName = HBaseSparkFormatUtils.splitColumnAndQualifierName(field.name)
          (Bytes.toBytes(separateName.familyName),
            Bytes.toBytes(separateName.qualifierName),
            idx,
            HBaseSparkDataUtils.genHBaseFieldConverter(field.dataType))
        }
        proj.initialize(index)
        val size = schema.length
        val rowKey = zippedSchema.find(_._1.name == TABLE_CONSTANTS.ROW_KEY.getValue)
        if (rowKey.isDefined) {
          //需要rowKey数据
          val rowIdx = rowKey.get._2
          val filteredColumnFamily = columnFamily.filter(_._3 != rowIdx)
          iter.map { result =>
            val r = HBaseSparkDataUtils.hbaseResult2InternalRowWithRowKey(result._2, size, filteredColumnFamily, columnFamily(rowIdx))
            numOutputRows += 1
            proj(r)
          }
        } else {
          //不需要rowKey数据
          iter.map { result =>
            val r = HBaseSparkDataUtils.hbaseResult2InternalRowWithoutRowKey(result._2, size, columnFamily)
            numOutputRows += 1
            proj(r)
          }
        }
      }
  }


  override def otherCopyArgs: Seq[AnyRef] = Seq(hbaseSession)

  //columnFamily_QualifierName <=== requestAttribute
  def addColumnFamiliesToScan(scan: Scan, filters: Option[Filter], predicate: Seq[Expression]): Scan = {
    // 添加需要显示结果的列
    requestedAttributes.foreach { attribute =>
      val separateName = HBaseSparkFormatUtils.splitColumnAndQualifierName(attribute.name)
      if (attribute.name != TABLE_CONSTANTS.ROW_KEY.getValue)
        scan.addColumn(Bytes.toBytes(separateName.familyName), Bytes.toBytes(separateName.qualifierName))
    }
    // 添加需要过滤的列
    predicate.foreach { expression =>
      val attributes = expression.references.map(_.toAttribute).toSet
      attributes.filter(_.name != TABLE_CONSTANTS.ROW_KEY.getValue).foreach { attribute =>
        val separateName = HBaseSparkFormatUtils.splitColumnAndQualifierName(attribute.name)
        scan.addColumn(Bytes.toBytes(separateName.familyName), Bytes.toBytes(separateName.qualifierName))
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
