package org.apache.spark.sql.hbase.execution

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CompareOperator, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeReference, AttributeSeq, AttributeSet, Cast, Contains, EndsWith, EqualTo, Expression, GenericInternalRow, GreaterThan, GreaterThanOrEqual, InSet, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, NamedExpression, Or, StartsWith, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.QueryPlan
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
                               plan: HBasePlan,
                               filter: Seq[Expression])(
                               @transient private val hbaseSession: HBaseSession)
  extends LeafExecNode {
  val meta = plan.tableMeta
  val parameters = meta.properties
  val tableName = meta.identifier.database.get + ":" + meta.identifier.table

  private val HBASE_ROW_BYTES = Bytes.toBytes("row")
  private val HBASE_KEY_BYTES = Bytes.toBytes("key")

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

    //TODO need more smaller scans rather than the map split
    val scan = new Scan()
    val numOutputRows = longMetric("numOutputRows")
    val hbaseFilter = buildHBaseFilterList4Where(filter.headOption)
    addColumnFamiliesToScan(scan, hbaseFilter, filter.headOption, requestedAttributes)
    val dataTypes = schema.map(_.dataType)
    hbaseSession.sqlContext.hbaseRDD(TableName.valueOf(tableName), scan)
      .mapPartitionsWithIndexInternal { (index, iter) =>
        val proj = UnsafeProjection.create(schema)
        val columnFamily = schema.map { field =>
          val cf_q = field.name.split("_")
          (Bytes.toBytes(cf_q.head), Bytes.toBytes(cf_q.last), genHBaseFieldConverter(field.dataType))
        }
        proj.initialize(index)
        val size = schema.length
        iter.map { result =>
          val r = hbase2SparkRow(result._2, size, columnFamily)
          numOutputRows += 1
          proj(r)
        }
      }
  }


  override def otherCopyArgs: Seq[AnyRef] = Seq(hbaseSession)

  type CF_QUALIFIER_CONVERTER = (Array[Byte], Array[Byte], (InternalRow, Int, Array[Byte]) => Unit)

  def genHBaseFieldConverter(dataType: DataType): (InternalRow, Int, Array[Byte]) => Unit = dataType match {
    case ByteType =>
      (internalRow, i, v) => internalRow.update(i, v.head)

    case StringType =>
      (internalRow, i, v) => internalRow.update(i, UTF8String.fromBytes(v))

    //convert to milli seconds
    case TimestampType =>
      (internalRow, i, v) => internalRow.update(i, Bytes.toLong(v) * 1000)
    case LongType =>
      (internalRow, i, v) => internalRow.update(i, Bytes.toLong(v))
    case IntegerType =>
      (internalRow, i, v) => internalRow.update(i, Bytes.toInt(v))
    case ShortType =>
      (internalRow, i, v) => internalRow.update(i, Bytes.toShort(v))

    case BooleanType =>
      (internalRow, i, v) => internalRow.update(i, Bytes.toBoolean(v))

    case DoubleType =>
      (internalRow, i, v) => internalRow.update(i, Bytes.toDouble(v))
    case FloatType =>
      (internalRow, i, v) => internalRow.update(i, Bytes.toFloat(v))

    case _ =>
      (internalRow, i, v) => internalRow.update(i, v)
  }

  def hbase2SparkRow(result: Result, size: Int, cols: Seq[CF_QUALIFIER_CONVERTER]): InternalRow = {
    var i = 0
    val internalRow = new GenericInternalRow(size)
    cols.foreach { case (family, qualifier, convert) =>
      val v = if (Bytes.equals(family, HBASE_ROW_BYTES) && Bytes.equals(qualifier, HBASE_KEY_BYTES))
        result.getRow
      else
        result.getValue(family, qualifier)

      if (v == null) internalRow.update(i, null)
      else convert(internalRow, i, v)
      i += 1
    }
    internalRow
  }

  //columnFamily_QualifierName <=== requestAttribute
  def addColumnFamiliesToScan(scan: Scan, filters: Option[Filter], predicate: Option[Expression], projectionList: Seq[NamedExpression]): Scan = {
    requestedAttributes.foreach { qualifier =>
      val column_qualifier = qualifier.name.split("_", 2)
      if (qualifier.name != "row_key")
        scan.addColumn(column_qualifier.head.getBytes, column_qualifier.last.getBytes)
    }
    scan.setCaching(1000)
    scan.readAllVersions()
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
      val filter = new SingleColumnValueFilter(Bytes.toBytes(col_qualifier.head), Bytes.toBytes(col_qualifier.last), CompareOperator.EQUAL, new NullComparator())
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
      val filter = new SingleColumnValueFilter(Bytes.toBytes(col_qualifier.head), Bytes.toBytes(col_qualifier.last), CompareOperator.NOT_EQUAL, new NullComparator())
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

  def createSingleColumnValueFilter(left: AttributeReference, right: Literal, compareOp: CompareOperator, comparable: ByteArrayComparable = null): Option[FilterList] = {
    val nonKeyColumn = requestedAttributes.find(_.name == left.name)

    if (nonKeyColumn.isDefined) {
      val column = nonKeyColumn.get.name.split("_", 2)
      val nullComparable = comparable == null

      var filter = new SingleColumnValueFilter(Bytes.toBytes(column.head), Bytes.toBytes(column.last), compareOp, new BinaryComparator(getBinaryValue(right)))
      val filter1 = new SingleColumnValueFilter(Bytes.toBytes(column.head), Bytes.toBytes(column.last), compareOp, comparable)

      if (!nullComparable) filter = filter1

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
                Bytes.toBytes(col_qualifier.head), Bytes.toBytes(col_qualifier.last), CompareOperator.EQUAL,
                new BinaryComparator(Bytes.toBytes(item.asInstanceOf[String])))
              filterList.addFilter(filter)
            }
            Some(filterList)
          }
          else None

        case IsNull(left: AttributeReference) => createNullFilter(left)
        case IsNotNull(left: AttributeReference) => createNotNullFilter(left)
        case GreaterThan(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareOperator.GREATER)
        case GreaterThan(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareOperator.GREATER)
        case GreaterThanOrEqual(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareOperator.GREATER_OR_EQUAL)
        case GreaterThanOrEqual(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareOperator.GREATER_OR_EQUAL)
        case EqualTo(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareOperator.EQUAL)
        case EqualTo(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareOperator.EQUAL)
        case EqualTo(left: Cast, right: Literal) =>
          val leftValue: AttributeReference = left.child.asInstanceOf[AttributeReference]
          val rightDecimal = BigDecimal(right.value.toString).bigDecimal
          val rightValue: Literal = Literal(rightDecimal.stripTrailingZeros().toPlainString)
          createSingleColumnValueFilter(leftValue, rightValue, CompareOperator.EQUAL)
        case LessThan(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareOperator.LESS)
        case LessThan(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareOperator.LESS)
        case LessThanOrEqual(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareOperator.LESS_OR_EQUAL)
        case LessThanOrEqual(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareOperator.LESS_OR_EQUAL)
        case StartsWith(left: AttributeReference, right: Literal) =>
          val regexStringComparator = new RegexStringComparator(".*" + right.value + "$")
          createSingleColumnValueFilter(left, right, CompareOperator.EQUAL, regexStringComparator)
        case EndsWith(left: AttributeReference, right: Literal) =>
          val binaryPrefixComparator = new BinaryPrefixComparator(Bytes.toBytes(right.value.toString))
          createSingleColumnValueFilter(left, right, CompareOperator.EQUAL, binaryPrefixComparator)
        case Contains(left: AttributeReference, right: Literal) =>
          val substringComparator = new SubstringComparator(right.value.toString)
          createSingleColumnValueFilter(left, right, CompareOperator.EQUAL, substringComparator)
        case _ => None
      }
    }
  }

}
