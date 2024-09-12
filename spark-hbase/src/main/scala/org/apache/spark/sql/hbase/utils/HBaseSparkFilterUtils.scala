package org.apache.spark.sql.hbase.utils

import org.apache.hadoop.hbase.CompareOperator
import org.apache.hadoop.hbase.filter.{BinaryComparator, BinaryPrefixComparator, ByteArrayComparable, Filter, FilterList, NullComparator, RegexStringComparator, RowFilter, SingleColumnValueFilter, SubstringComparator}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, Cast, Contains, EndsWith, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, InSet, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Or, StartsWith}
import org.apache.spark.sql.hbase.SparkHBaseConstants.TABLE_CONSTANTS
import org.apache.spark.sql.types.{BooleanType, ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType}
import org.apache.spark.unsafe.types.UTF8String

object HBaseSparkFilterUtils {

  def combineHBaseFilterLists(filterLists: Seq[Option[FilterList]]): Option[Filter] = {
    val filterList = new FilterList()
    filterLists.foreach { case Some(filter) => filterList.addFilter(filter) }
    if (filterList.getFilters.isEmpty) {
      None
    } else {
      Some(filterList)
    }
  }

  def buildHBaseFilterList4Where(
                                  dataCols: Seq[Attribute],
                                  requestedAttributes: Seq[Attribute],
                                  filter: Option[Expression]): Option[FilterList] = {
    if (filter.isEmpty) {
      None
    } else {
      val expression = filter.get
      expression match {
        case And(left, right) =>
          val filters = new java.util.ArrayList[Filter]

          if (left != null) {
            val leftFilterList = buildHBaseFilterList4Where(dataCols, requestedAttributes, Some(left))
            add2FilterList(filters, leftFilterList, FilterList.Operator.MUST_PASS_ALL)
          }
          if (right != null) {
            val rightFilterList = buildHBaseFilterList4Where(dataCols, requestedAttributes, Some(right))
            add2FilterList(filters, rightFilterList, FilterList.Operator.MUST_PASS_ALL)
          }
          Some(new FilterList(FilterList.Operator.MUST_PASS_ALL, filters))

        case Or(left, right) =>
          val filters = new java.util.ArrayList[Filter]
          if (left != null) {
            val leftFilterList = buildHBaseFilterList4Where(dataCols, requestedAttributes, Some(left))
            add2FilterList(filters, leftFilterList, FilterList.Operator.MUST_PASS_ONE)
          }
          if (right != null) {
            val rightFilterList = buildHBaseFilterList4Where(dataCols, requestedAttributes, Some(right))
            add2FilterList(filters, rightFilterList, FilterList.Operator.MUST_PASS_ONE)
          }
          Some(new FilterList(FilterList.Operator.MUST_PASS_ONE, filters))

        case InSet(value@AttributeReference(name, dataType, _, _), hset) =>
          dataCols.find(_.name == name) match {
            case Some(attribute) =>
              val separateName = HBaseSparkFormatUtils.splitColumnAndQualifierName(attribute.name)
              val filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
              hset.foreach { item =>
                val filter = new SingleColumnValueFilter(
                  Bytes.toBytes(separateName.familyName), Bytes.toBytes(separateName.qualifierName), CompareOperator.EQUAL,
                  new BinaryComparator(HBaseSparkDataUtils.toBytes(item, dataType)))
                filterList.addFilter(filter)
              }
              Some(filterList)
            case _ => None
          }

        case IsNull(left: AttributeReference) =>
          createNullFilter(requestedAttributes, left)

        case IsNotNull(left: AttributeReference) =>
          createNotNullFilter(requestedAttributes, left)

        case GreaterThan(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(dataCols, left, right, CompareOperator.GREATER)

        case GreaterThan(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(dataCols, right, left, CompareOperator.GREATER)

        case GreaterThanOrEqual(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(dataCols, left, right, CompareOperator.GREATER_OR_EQUAL)

        case GreaterThanOrEqual(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(dataCols, right, left, CompareOperator.GREATER_OR_EQUAL)

        case EqualTo(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(dataCols, left, right, CompareOperator.EQUAL)

        case EqualTo(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(dataCols, right, left, CompareOperator.EQUAL)

        case EqualTo(left: Cast, right: Literal) =>
          val leftValue: AttributeReference = left.child.asInstanceOf[AttributeReference]
          val rightDecimal = BigDecimal(right.value.toString).bigDecimal
          val rightValue: Literal = Literal(rightDecimal.stripTrailingZeros().toPlainString)
          createSingleColumnValueFilter(dataCols, leftValue, rightValue, CompareOperator.EQUAL)

        case LessThan(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(dataCols, left, right, CompareOperator.LESS)

        case LessThan(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(dataCols, right, left, CompareOperator.LESS)

        case LessThanOrEqual(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(dataCols, left, right, CompareOperator.LESS_OR_EQUAL)

        case LessThanOrEqual(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(dataCols, right, left, CompareOperator.LESS_OR_EQUAL)

        case StartsWith(left: AttributeReference, right: Literal) =>
          val regexStringComparator = new RegexStringComparator(".*" + right.value + "$")
          createSingleColumnValueFilter(dataCols, left, right, CompareOperator.EQUAL, regexStringComparator)

        case EndsWith(left: AttributeReference, right: Literal) =>
          val binaryPrefixComparator = new BinaryPrefixComparator(Bytes.toBytes(right.value.toString))
          createSingleColumnValueFilter(dataCols, left, right, CompareOperator.EQUAL, binaryPrefixComparator)

        case Contains(left: AttributeReference, right: Literal) =>
          val substringComparator = new SubstringComparator(right.value.toString)
          createSingleColumnValueFilter(dataCols, left, right, CompareOperator.EQUAL, substringComparator)

        case _ => None
      }
    }
  }

  def createNullFilter(
                        requestedAttributes: Seq[Attribute],
                        left: AttributeReference): Option[FilterList] = {
    requestedAttributes.find(_.name == left.name) match {
      case Some(attribute) =>
        val separateName = HBaseSparkFormatUtils.splitColumnAndQualifierName(attribute.name)
        val filter = new SingleColumnValueFilter(
          Bytes.toBytes(separateName.familyName),
          Bytes.toBytes(separateName.qualifierName),
          CompareOperator.EQUAL,
          new NullComparator())
        filter.setFilterIfMissing(true)
        Some(new FilterList(filter))
      case _ => None
    }
  }

  def createNotNullFilter(
                           requestedAttributes: Seq[Attribute],
                           left: AttributeReference): Option[FilterList] = {
    requestedAttributes.find(_.name == left.name) match {
      case Some(attribute) =>
        val separateName = HBaseSparkFormatUtils.splitColumnAndQualifierName(attribute.name)
        val filter = new SingleColumnValueFilter(
          Bytes.toBytes(separateName.familyName),
          Bytes.toBytes(separateName.qualifierName),
          CompareOperator.NOT_EQUAL,
          new NullComparator())
        filter.setFilterIfMissing(true)
        Some(new FilterList(filter))

      case _ => None
    }
  }

    def createSingleColumnValueFilter(
                                       dataCols: Seq[Attribute],
                                       left: AttributeReference,
                                       right: Literal,
                                       compareOp: CompareOperator,
                                       comparable: ByteArrayComparable = null): Option[FilterList] = {
      dataCols.find(_.name == left.name) match {
        case Some(attribute) =>
          val hbaseFilter = if (attribute.name == TABLE_CONSTANTS.ROW_KEY.getValue) {
            // 当使用row_key作为过滤条件时使用RowFilter加快查询速度
            if (comparable != null) {
              new RowFilter(compareOp, comparable)
            } else {
              new RowFilter(compareOp, new BinaryComparator(getBinaryValue(right)))
            }
          } else {
            val separateName = HBaseSparkFormatUtils.splitColumnAndQualifierName(attribute.name)
            val kvFilter = if (comparable != null) {
              new SingleColumnValueFilter(
                Bytes.toBytes(separateName.familyName),
                Bytes.toBytes(separateName.qualifierName),
                compareOp,
                comparable)
            } else {
              new SingleColumnValueFilter(
                Bytes.toBytes(separateName.familyName),
                Bytes.toBytes(separateName.qualifierName),
                compareOp,
                new BinaryComparator(getBinaryValue(right)))
            }
            // 这里设置为false会导致过滤条件为cf2但返回结果不包含cf2时，hbase不进行过滤
            kvFilter.setFilterIfMissing(true)
            kvFilter
          }
          Some(new FilterList(hbaseFilter))

        case _ =>
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

    private def add2FilterList(
                                filters: java.util.ArrayList[Filter],
                                filtersToBeAdded: Option[FilterList],
                                operator: FilterList.Operator): Unit = {
      import collection.JavaConverters._
      if (filtersToBeAdded.isDefined) {
        val filterList = filtersToBeAdded.get
        val size = filterList.getFilters.size
        if (size == 1 || filterList.getOperator == operator) {
          filterList.getFilters.asScala.foreach(filters.add)
        } else {
          filters.add(filterList)
        }
      }
    }
  }
