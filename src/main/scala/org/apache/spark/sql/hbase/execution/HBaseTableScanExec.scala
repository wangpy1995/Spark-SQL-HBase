package org.apache.spark.sql.hbase.execution

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeReference, AttributeSet, Cast, Contains, EndsWith, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, InSet, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, NamedExpression, Or, StartsWith, UnsafeProjection}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by wpy on 17-5-16.
  */
private[hbase]
case class HBaseTableScanExec(
                               requestedAttributes: Seq[Attribute],
                               relation: HBaseRelation,
                               filter: Seq[Expression])(
                               @transient private val hbaseSession: HBaseSession)
  extends LeafExecNode {
  val meta = relation.tableMeta
  val parameters = meta.properties
  val tableName = meta.identifier.database.get + ":" + meta.identifier.table

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def producedAttributes: AttributeSet = outputSet ++
    AttributeSet(filter.flatMap(_.references))

  private val originalAttributes = AttributeMap(relation.output.map(a => a -> a))

  override val output: Seq[Attribute] = {
    // Retrieve the original attributes based on expression ID so that capitalization matches.
    requestedAttributes.map(originalAttributes)
  }


  override protected def doExecute(): RDD[InternalRow] = {

    //TODO need more smaller scans rather than the map split
    val scan = new Scan()
    val hbaseFilter = buildHBaseFilterList4Where(filter.headOption)
    addColumnFamiliesToScan(scan, hbaseFilter, filter.headOption, requestedAttributes)
    val dataTypes = schema.map(_.dataType)
    hbaseSession.sqlContext.hbaseRDD(TableName.valueOf(tableName), scan)
      .map(_._2.rawCells()).map { cells =>
      UnsafeProjection.create(schema)(
        InternalRow.fromSeq(for (i <- cells.indices) yield {
          CatalystTypeConverters.createToCatalystConverter(dataTypes(i))(UTF8String.fromBytes(CellUtil.cloneValue(cells(i))))
        })
      )
    }
  }

  //columnFamily_QualifierName <=== requestAttribute
  def addColumnFamiliesToScan(scan: Scan, filters: Option[Filter], predicate: Option[Expression], projectionList: Seq[NamedExpression]): Scan = {
    requestedAttributes.foreach { qualifier =>
      val column_qualifier = qualifier.name.split("_", 2)
      scan.addColumn(column_qualifier.head.getBytes, column_qualifier.last.getBytes)
    }
    scan.setCaching(1000)
    scan.setMaxVersions()
    if (filters.isDefined) {
      scan.setFilter(filters.get)
    }
    scan
  }


  private def add2FilterList(filters: java.util.ArrayList[Filter], filtersToBeAdded: Option[FilterList], operator: FilterList.Operator) = {
    import collection.JavaConverters._
    if (filtersToBeAdded.isDefined) {
      val filterList = filtersToBeAdded.get
      val size = filterList.getFilters.size
      if (size == 1 || filterList.getOperator == operator) {
        filterList.getFilters.asScala.map(filters.add)
      }
      else {
        filters.add(filterList)
      }
    }
  }

  def createNullFilter(left: AttributeReference): Option[FilterList] = {
    val Column = requestedAttributes.find(_.name == left.name)
    if (Column.isDefined) {
      val col_qualifier = Column.get.name.split("_", 2)
      val filter = new SingleColumnValueFilter(Bytes.toBytes(col_qualifier.head), Bytes.toBytes(col_qualifier.last), CompareFilter.CompareOp.EQUAL, new NullComparator())
      filter.setFilterIfMissing(true)
      Some(new FilterList(filter))
    }
    else {
      None
    }
  }

  def createNotNullFilter(left: AttributeReference): Option[FilterList] = {
    val Column = requestedAttributes.find(_.name == left.name)
    if (Column.isDefined) {
      val col_qualifier = Column.get.name.split("_", 2)
      val filter = new SingleColumnValueFilter(Bytes.toBytes(col_qualifier.head), Bytes.toBytes(col_qualifier.last), CompareFilter.CompareOp.NOT_EQUAL, new NullComparator())
      filter.setFilterIfMissing(true)
      Some(new FilterList(filter))
    }
    else {
      None
    }
  }


  private def getBinaryValue(literal: Literal): Array[Byte] = {
    literal.dataType match {
      case BooleanType => Bytes.toBytes(literal.value.asInstanceOf[Boolean])
      case ByteType => Bytes.toBytes(literal.value.asInstanceOf[Byte])
      case ShortType => Bytes.toBytes(literal.value.asInstanceOf[Short])
      case IntegerType => Bytes.toBytes(literal.value.asInstanceOf[Int])
      case LongType => Bytes.toBytes(literal.value.asInstanceOf[Long])
      case FloatType => Bytes.toBytes(literal.value.asInstanceOf[Float])
      case DoubleType => Bytes.toBytes(literal.value.asInstanceOf[Double])
      case StringType => UTF8String.fromString(literal.value.toString).getBytes
    }
  }

  def createSingleColumnValueFilter(left: AttributeReference, right: Literal, compareOp: CompareFilter.CompareOp, Comparable: ByteArrayComparable = null): Option[FilterList] = {
    val nonKeyColumn = requestedAttributes.find(_.name == left.name)

    if (nonKeyColumn.isDefined) {
      val column = nonKeyColumn.get.name.split("_", 2)
      val filter = if (Comparable == null)
        new SingleColumnValueFilter(Bytes.toBytes(column.head), Bytes.toBytes(column.last), compareOp, new BinaryComparator(getBinaryValue(right)))
      else
        new SingleColumnValueFilter(Bytes.toBytes(column.head), Bytes.toBytes(column.last), compareOp, Comparable)

      filter.setFilterIfMissing(true)
      Some(new FilterList(filter))
    }
    else {
      None
    }
  }

  def buildHBaseFilterList4Where(filter: Option[Expression]): Option[FilterList] = {
    if (filter.isEmpty) {
      None
    }
    else {
      val expression = filter.get
      expression match {
        case And(left, right) =>
          val filters = new java.util.ArrayList[Filter]

          if (left != null) {
            val leftFilterList = buildHBaseFilterList4Where(Some(left))
            add2FilterList(filters, leftFilterList, FilterList.Operator.MUST_PASS_ALL)
          }
          if (right != null) {
            val rightFilterList = buildHBaseFilterList4Where(Some(right))
            add2FilterList(filters, rightFilterList, FilterList.Operator.MUST_PASS_ALL)
          }
          Some(new FilterList(FilterList.Operator.MUST_PASS_ALL, filters))

        case Or(left, right) =>
          val filters = new java.util.ArrayList[Filter]
          if (left != null) {
            val leftFilterList = buildHBaseFilterList4Where(Some(left))
            add2FilterList(filters, leftFilterList, FilterList.Operator.MUST_PASS_ONE)
          }
          if (right != null) {
            val rightFilterList = buildHBaseFilterList4Where(Some(right))
            add2FilterList(filters, rightFilterList, FilterList.Operator.MUST_PASS_ONE)
          }
          Some(new FilterList(FilterList.Operator.MUST_PASS_ONE, filters))

        case InSet(value@AttributeReference(name, dataType, _, _), hset) =>
          val column = requestedAttributes.find(_.name == name)
          if (column.isDefined) {
            val col_qualifier = column.get.name.split("_", 2)
            val filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
            for (item <- hset) {
              val filter = new SingleColumnValueFilter(
                Bytes.toBytes(col_qualifier.head), Bytes.toBytes(col_qualifier.last), CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(item.asInstanceOf[String])))
              filterList.addFilter(filter)
            }
            Some(filterList)
          }
          else None

        case IsNull(left: AttributeReference) => createNullFilter(left)
        case IsNotNull(left: AttributeReference) => createNotNullFilter(left)
        case GreaterThan(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareFilter.CompareOp.GREATER)
        case GreaterThan(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareFilter.CompareOp.GREATER)
        case GreaterThanOrEqual(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareFilter.CompareOp.GREATER_OR_EQUAL)
        case GreaterThanOrEqual(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareFilter.CompareOp.GREATER_OR_EQUAL)
        case EqualTo(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareFilter.CompareOp.EQUAL)
        case EqualTo(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareFilter.CompareOp.EQUAL)
        case EqualTo(left: Cast, right: Literal) =>
          val leftValue: AttributeReference = left.child.asInstanceOf[AttributeReference]
          val rightDecimal = BigDecimal(right.value.toString).bigDecimal
          val rightValue: Literal = Literal(rightDecimal.stripTrailingZeros().toPlainString)
          createSingleColumnValueFilter(leftValue, rightValue, CompareFilter.CompareOp.EQUAL)
        case LessThan(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareFilter.CompareOp.LESS)
        case LessThan(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareFilter.CompareOp.LESS)
        case LessThanOrEqual(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareFilter.CompareOp.LESS_OR_EQUAL)
        case LessThanOrEqual(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareFilter.CompareOp.LESS_OR_EQUAL)
        case StartsWith(left: AttributeReference, right: Literal) =>
          val regexStringComparator = new RegexStringComparator(".*" + right.value + "$")
          createSingleColumnValueFilter(left, right, CompareFilter.CompareOp.EQUAL, regexStringComparator)
        case EndsWith(left: AttributeReference, right: Literal) =>
          val binaryPrefixComparator = new BinaryPrefixComparator(Bytes.toBytes(right.value.toString))
          createSingleColumnValueFilter(left, right, CompareFilter.CompareOp.EQUAL, binaryPrefixComparator)
        case Contains(left: AttributeReference, right: Literal) =>
          val substringComparator = new SubstringComparator(right.value.toString)
          createSingleColumnValueFilter(left, right, CompareFilter.CompareOp.EQUAL, substringComparator)
        case _ => None
      }
    }
  }

}
