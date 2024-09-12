package org.apache.spark.sql.hbase.execution

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Mutation, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.hbase.SparkHBaseConstants.TABLE_CONSTANTS
import org.apache.spark.sql.hbase.utils.{HBaseSparkDataUtils, HBaseSparkFormatUtils}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StructField, StructType}

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

class HBaseTableFormat
  extends FileFormat
    with DataSourceRegister
    with Serializable
    with Logging {

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
    formatter.format(new Date())
  }

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    throw new UnsupportedOperationException("inferSchema is not supported for hbase data source.")
  }

/*  override def buildReader(sparkSession: SparkSession,
                           dataSchema: StructType,
                           partitionSchema: StructType,
                           requiredSchema: StructType,
                           filters: Seq[Filter],
                           options: Map[String, String],
                           hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val conf = HBaseConfiguration.create(hadoopConf)
    val inputFormat = new TableInputFormat()
    inputFormat.setConf(conf)
    val requiredEmpty = requiredSchema.isEmpty
    if (requiredEmpty) {
      _ => Iterator.empty
    } else {
      val len = requiredSchema.length
      val rowKeyIdx = requiredSchema.getFieldIndex(TABLE_CONSTANTS.ROW_KEY.getValue).getOrElse(-1)
      val dataColumns = requiredSchema.map { field =>
        val separateName = HBaseSparkFormatUtils.splitColumnAndQualifierName(field.name)
        val family = Bytes.toBytes(separateName.familyName)
        val qualifier = Bytes.toBytes(separateName.qualifierName)
        val converter = HBaseSparkDataUtils.genHBaseToInternalRowConverter(field.dataType)
        val idx = requiredSchema.getFieldIndex(field.name).get
        (family, qualifier, idx, converter)
      }
      val id = sparkSession.sparkContext.newRddId()
      //return serializable function
      hTable => {
        new Iterator[InternalRow] {
          //all fields must be serialized
          private val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, 0, 0)
          private val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
          val hTableReader: RecordReader[ImmutableBytesWritable, Result] = inputFormat.createRecordReader(null, hadoopAttemptContext)
          val scanner = hTableReader
          var hasNextValue: Boolean = false

          override def hasNext: Boolean = {
            hasNextValue = scanner.nextKeyValue()
            hasNextValue
          }

          override def next(): InternalRow = {
            if (requiredEmpty) {
              new GenericInternalRow(0)
            } else {
              val row = new GenericInternalRow(len)
              val result = scanner.getCurrentValue
              if (rowKeyIdx != -1) {
                HBaseSparkDataUtils.hbaseResult2InternalRowWithRowKey(result,len,dataColumns,dataColumns(rowKeyIdx))
              }else{
                HBaseSparkDataUtils.hbaseResult2InternalRowWithoutRowKey(result,len,dataColumns)
              }
              row
            }
          }
        }
      }
    }
  }*/

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    val dirPath = new Path(options("path"))
    val tableName = dirPath.getName
    val namespace = dirPath.getParent.getName
    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = null

      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        val conf = HBaseConfiguration.create()
        conf.set(TableOutputFormat.OUTPUT_TABLE, s"$namespace:$tableName")
        new HBaseTableOutputWriter(conf, context, dataSchema)
      }
    }

  }

  override def shortName(): String = "hbase"
}

class HBaseTableOutputWriter(conf: Configuration, context: TaskAttemptContext, dataSchema: StructType) extends OutputWriter {
  val tableOutputFormat = new TableOutputFormat[ImmutableBytesWritable]()
  tableOutputFormat.setConf(conf)
  val tableRecordWriter: RecordWriter[ImmutableBytesWritable, Mutation] = tableOutputFormat.getRecordWriter(context)

  val rowKeyIdx: Int = dataSchema.getFieldIndex(TABLE_CONSTANTS.ROW_KEY.getValue).get
  val filteredDataSchema: Seq[StructField] = dataSchema.filter(_.name != TABLE_CONSTANTS.ROW_KEY.getValue)
  val rowKeyConverter: (InternalRow, Int) => Array[Byte] = HBaseSparkDataUtils.interRowToHBaseFunc(dataSchema(rowKeyIdx).dataType)

  val schemaMap: Map[String, (Array[Byte], Array[Byte], Int, (InternalRow, Int) => Array[Byte])] = filteredDataSchema.map { field =>
    val separateName = HBaseSparkFormatUtils.splitColumnAndQualifierName(field.name)
    field.name -> (
      Bytes.toBytes(separateName.familyName),
      Bytes.toBytes(separateName.qualifierName),
      dataSchema.getFieldIndex(field.name).get,
      HBaseSparkDataUtils.interRowToHBaseFunc(field.dataType))
  }.toMap


  override def write(row: InternalRow): Unit = {
    if (row.numFields > 0) {
      val rowKey = rowKeyConverter(row, rowKeyIdx)
      val put = new Put(rowKey)
      filteredDataSchema.foreach { field =>
        val (family, qualifier, idx, converter) = schemaMap(field.name)
        put.addColumn(family, qualifier, converter(row, idx))
      }
      tableRecordWriter.write(null, put)
    }
  }

  override def close(): Unit = tableRecordWriter.close(context)

  override def path(): String = {
    val name = context.getConfiguration.get("mapreduce.output.fileoutputformat.outputdir")
    if (name == null) {
      null
    } else {
      name
    }
  }
}