package org.apache.spark.sql.hbase.execution

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.hbase.CellBuilderType
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._

class HBaseFileFormat extends FileFormat with DataSourceRegister with Logging {

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    throw new UnsupportedOperationException("inferSchema is not supported for hbase data source.")
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = "hfile"

      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = new HBaseOutputWriter(context, dataSchema)
    }

  }

  override def shortName(): String = "hbase"
}

class HBaseOutputWriter(context: TaskAttemptContext, dataSchema: StructType) extends OutputWriter {
  val hFileWriter = new HFileOutputFormat2().getRecordWriter(context)

  val rowKeyIdx = dataSchema.getFieldIndex("rowKey").get
  val rowKeyConverter = genInternalRowToHBaseConverter(dataSchema(rowKeyIdx).dataType)

  val schemaMap = dataSchema.map { field =>
    val familyQualifierName = field.name.split("_", 2)
    val familyName = Bytes.toBytes(familyQualifierName.head)
    val qualifierName = Bytes.toBytes(familyQualifierName.last)
    field.name -> (familyName, qualifierName, dataSchema.getFieldIndex(field.name), genInternalRowToHBaseConverter(field.dataType))
  }.toMap

  def genInternalRowToHBaseConverter(dataType: DataType): (InternalRow, Int) => Array[Byte] = dataType match {
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

  override def write(row: InternalRow): Unit = {
    if (row.numFields > 0) {
      val rowKey = rowKeyConverter(row, rowKeyIdx)
      val put = new Put(rowKey)
      dataSchema.foreach { field =>
        val (family, qualifier, idx, converter) = schemaMap(field.name)
        put.addColumn(family, qualifier, converter(row, idx.get))
      }
      hFileWriter.write(null, put.getCellBuilder(CellBuilderType.SHALLOW_COPY).build())
    }
  }

  override def close(): Unit = hFileWriter.close(context)

  override def path(): String = {
    val name = context.getConfiguration.get("mapreduce.output.fileoutputformat.outputdir")
    if (name == null) {
      null
    } else {
      name
    }
  }
}