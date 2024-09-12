package org.apache.spark.sql.hbase.utils

import org.apache.hadoop.hbase.client.{RegionInfo, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.hbase.types.RegionInfoUDT
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ObjectType, ShortType, StringType, StructType, TimestampType, UserDefinedType}
import org.apache.spark.unsafe.types.UTF8String

object HBaseSparkDataUtils extends Serializable {
  // column family, qualifier, a function that could transform bytes data and set into InternalRow
  type CF_QUALIFIER_CONVERTER = (Array[Byte], Array[Byte], Int, (InternalRow, Int, Array[Byte]) => Unit)

  private val HBASE_ROW_BYTES = Bytes.toBytes("row_key")

  /**
   * generate a data converter, this converter could get data from
   * spark [InternalRow]  and transform data to HBase Bytes Value with dataType
   *
   * @param dataType 数据类型
   * @return
   */
  def interRowToHBaseFunc(dataType: DataType): (InternalRow, Int) => Array[Byte] = dataType match {
    case ByteType =>
      (internalRow, i) => Array(internalRow.getByte(i))

    case StringType =>
      (internalRow, i) => Bytes.toBytes(internalRow.getUTF8String(i).toString)

    //convert to milli seconds
    case TimestampType =>
      (internalRow, i) => Bytes.toBytes(internalRow.getLong(i) / 1000)
    case LongType =>
      (internalRow, i) => Bytes.toBytes(internalRow.getLong(i))
    case IntegerType =>
      (internalRow, i) => Bytes.toBytes(internalRow.getInt(i))
    case ShortType =>
      (internalRow, i) => Bytes.toBytes(internalRow.getShort(i))

    case BooleanType =>
      (internalRow, i) => Bytes.toBytes(internalRow.getBoolean(i))

    case DoubleType =>
      (internalRow, i) => Bytes.toBytes(internalRow.getDouble(i))
    case FloatType =>
      (internalRow, i) => Bytes.toBytes(internalRow.getFloat(i))

    case _ =>
      (internalRow, i) => internalRow.getBinary(i)
  }

  def rowToHBaseFunc(dataType: DataType):(Row,Int)=>Array[Byte] = dataType match {
    case ByteType =>
      (row, i) => Array(row.getByte(i))

    case StringType =>
      (row, i) => Bytes.toBytes(row.getString(i))

    //convert to milli seconds
    case TimestampType =>
      (row, i) => Bytes.toBytes(row.getLong(i) / 1000)
    case LongType =>
      (row, i) => Bytes.toBytes(row.getLong(i))
    case IntegerType =>
      (row, i) => Bytes.toBytes(row.getInt(i))
    case ShortType =>
      (row, i) => Bytes.toBytes(row.getShort(i))

    case BooleanType =>
      (row, i) => Bytes.toBytes(row.getBoolean(i))

    case DoubleType =>
      (row, i) => Bytes.toBytes(row.getDouble(i))
    case FloatType =>
      (row, i) => Bytes.toBytes(row.getFloat(i))

    case _ =>
      (row, i) => Bytes.toBytes(row.getString(i))
  }

  def genInternalRowConverters(structType: StructType): Seq[(InternalRow, Int) => Array[Byte]] ={
    structType.map(_.dataType).map(interRowToHBaseFunc)
  }

  def genRowConverters(structType: StructType): Seq[(Row, Int) => Array[Byte]] ={
    structType.map(_.dataType).map(rowToHBaseFunc)
  }

  /**
   * generate a data converter that could transform HBase Bytes Value to Spark InternalRow
   *
   * @param dataType 数据类型
   * @return
   */
  def genHBaseToInternalRowConverter(dataType: DataType): (InternalRow, Int, Array[Byte]) => Unit = dataType match {
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

    case RegionInfoUDT =>
      (internalRow, i, v) => internalRow.update(i, v)

    case _ =>
      (internalRow, i, v) => internalRow.update(i, v)
  }

  def genHBaseToInternalRowConverterWithOffset(dataType: DataType): (InternalRow, Int, Array[Byte], Int, Int) => Unit = dataType match {
    case ByteType =>
      (internalRow, i, v, offset, len) => internalRow.update(i, v(offset))

    case StringType =>
      (internalRow, i, v, offset, len) => internalRow.update(i, UTF8String.fromBytes(v, offset, len))

    //convert to milli seconds
    case TimestampType =>
      (internalRow, i, v, offset, len) => internalRow.update(i, Bytes.toLong(v, offset) * 1000)
    case LongType =>
      (internalRow, i, v, offset, len) => internalRow.update(i, Bytes.toLong(v, offset))
    case IntegerType =>
      (internalRow, i, v, offset, len) => internalRow.update(i, Bytes.toInt(v, offset))
    case ShortType =>
      (internalRow, i, v, offset, len) => internalRow.update(i, Bytes.toShort(v, offset))

    case BooleanType =>
      (internalRow, i, v, offset, len) => internalRow.update(i, Bytes.toBoolean(Array(v(offset))))

    case DoubleType =>
      (internalRow, i, v, offset, len) => internalRow.update(i, Bytes.toDouble(v, offset))
    case FloatType =>
      (internalRow, i, v, offset, len) => internalRow.update(i, Bytes.toFloat(v, offset))

    case _ =>
      (internalRow, i, v, offset, len) => internalRow.update(i, Bytes.copy(v, offset, len))
  }


  /**
   * 对HBase中qualifier的数据进行转换
   * 不支持row_key
   *
   * @param result HBaseResult
   * @param size   查询列的个数
   * @param cols   查询的列集合(不含row_key)
   * @return
   */
  def hbaseResult2InternalRowWithoutRowKey(result: Result, size: Int, cols: Seq[CF_QUALIFIER_CONVERTER]): InternalRow = {
    val internalRow = new GenericInternalRow(size)
    cols.foreach { case (family, qualifier, idx, convert) =>
      val v = result.getValue(family, qualifier)
      if (v == null) {
        internalRow.update(idx, null)
      } else {
        convert(internalRow, idx, v)
      }
    }
    internalRow
  }

  def hbaseResult2InternalRowWithRowKey(result: Result,
                                        size: Int,
                                        cols: Seq[CF_QUALIFIER_CONVERTER],
                                        row: CF_QUALIFIER_CONVERTER): InternalRow = {
    val internalRow = hbaseResult2InternalRowWithoutRowKey(result, size, cols)
    row._4(internalRow, row._3, result.getRow)
    internalRow
  }


  def toBytes(data: Any, dataType: DataType): Array[Byte] = dataType match {
    case ByteType if data.isInstanceOf[Byte] =>
      Array(data.asInstanceOf[Byte])

    case ByteType if data.isInstanceOf[java.lang.Byte] =>
      Array(data.asInstanceOf[java.lang.Byte])

    case StringType if data.isInstanceOf[String] =>
      Bytes.toBytes(data.asInstanceOf[String])

    //convert to seconds
    case TimestampType if data.isInstanceOf[Long] =>
      Bytes.toBytes(data.asInstanceOf[Long] / 1000)

    case TimestampType if data.isInstanceOf[java.lang.Long] =>
      Bytes.toBytes(data.asInstanceOf[java.lang.Long] / 1000)

    case LongType if data.isInstanceOf[Long] =>
      Bytes.toBytes(data.asInstanceOf[Long])

    case TimestampType if data.isInstanceOf[Long] =>
      Bytes.toBytes(data.asInstanceOf[Long])

    case IntegerType if data.isInstanceOf[Int] =>
      Bytes.toBytes(data.asInstanceOf[Int])

    case TimestampType if data.isInstanceOf[java.lang.Integer] =>
      Bytes.toBytes(data.asInstanceOf[java.lang.Integer])

    case ShortType if data.isInstanceOf[Short] =>
      Bytes.toBytes(data.asInstanceOf[Short])

    case ShortType if data.isInstanceOf[java.lang.Short] =>
      Bytes.toBytes(data.asInstanceOf[java.lang.Short])

    case BooleanType if data.isInstanceOf[Boolean] =>
      Bytes.toBytes(data.asInstanceOf[Boolean])

    case BooleanType if data.isInstanceOf[java.lang.Boolean] =>
      Bytes.toBytes(data.asInstanceOf[java.lang.Boolean])

    case DoubleType if data.isInstanceOf[Double] =>
      Bytes.toBytes(data.asInstanceOf[Double])

    case DoubleType if data.isInstanceOf[java.lang.Double] =>
      Bytes.toBytes(data.asInstanceOf[java.lang.Double])

    case FloatType if data.isInstanceOf[Float] =>
      Bytes.toBytes(data.asInstanceOf[Float])

    case FloatType if data.isInstanceOf[java.lang.Float] =>
      Bytes.toBytes(data.asInstanceOf[java.lang.Float])

    case _ =>
      Bytes.toBytes(data.toString)
  }


}
