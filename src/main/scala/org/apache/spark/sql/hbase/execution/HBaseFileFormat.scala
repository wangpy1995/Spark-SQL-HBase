package org.apache.spark.sql.hbase.execution

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.CellBuilderType
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.hfile.HFile
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.hbase.utils.HBaseSparkDataUtils
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/**
 * 以Table为单位存取HFile文件
 */
class HBaseFileFormat
  extends FileFormat
    with DataSourceRegister
    with Serializable
    with Logging {

  private class HBaseRowArrayByteBuff(
                                       val bytes: Array[Byte],
                                       val offset: Int,
                                       val len: Int)
    extends Comparable[HBaseRowArrayByteBuff]
      with Serializable {
    def this(bytes: Array[Byte]) = this(bytes, 0, bytes.length)

    override def compareTo(t: HBaseRowArrayByteBuff): Int = Bytes.compareTo(
      this.bytes, this.offset, this.len,
      t.bytes, t.offset, t.len)

    override def toString: String = Bytes.toString(bytes, offset, len)
  }


  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    throw new UnsupportedOperationException("inferSchema is not supported for hbase data source.")
  }

  /**
   * read HFile
   *
   * @param sparkSession
   * @param dataSchema      data: HBase Qualifier
   * @param partitionSchema partition is not supported in HBase ,so empty partition here
   * @param requiredSchema  required HBase Qualifier
   * @param filters
   * @param options
   * @param hadoopConf
   * @return
   */
  override def buildReader(
                            sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val requiredQualifierNameMap = new java.util.TreeMap[HBaseRowArrayByteBuff, ((InternalRow, Int, Array[Byte], Int, Int) => Unit, Int)]()
    val rowKeyField = dataSchema.find(_.name == "row_key")
    assert(rowKeyField.isDefined)
    val rowKeyConverter = HBaseSparkDataUtils.genHBaseFieldConverterWithOffset(rowKeyField.get.dataType)
    val requiredEmpty = requiredSchema.isEmpty
    val requiredSchemaContainsRowKey = requiredSchema.exists(_.name == "row_key")
    val requiredRowKeyOnly = requiredSchema.length == 1 && requiredSchemaContainsRowKey
    val len = requiredSchema.length
    val rowKeyIdx = if (requiredSchemaContainsRowKey) requiredSchema.getFieldIndex("row_key").get else len

    requiredSchema.filter(_.name != "row_key").foreach { field =>
      val qualifier = new HBaseRowArrayByteBuff(Bytes.toBytes(field.name))
      val converter = HBaseSparkDataUtils.genHBaseFieldConverterWithOffset(field.dataType)
      val idx = requiredSchema.getFieldIndex(field.name).get
      requiredQualifierNameMap.put(qualifier, (converter, idx))
    }
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    hfile => {
      new Iterator[InternalRow] {
        var seeked = false
        val fs = FileSystem.get(broadcastedHadoopConf.value.value)
        val hFileReader = HFile.createReader(fs, new Path(hfile.filePath), broadcastedHadoopConf.value.value)
        val scanner = hFileReader.getScanner(false, false)
        var hashNextValue: Boolean = false

        override def hasNext: Boolean = {
          if (hashNextValue) {
            true
          } else {
            var hasNext: Boolean = false
            if (!seeked) {
              hasNext = scanner.seekTo()
              seeked = true
            } else {
              if (scanner.isSeeked) hasNext = scanner.next()
            }
            if (!hasNext) {
              hFileReader.close()
              false
            } else {
              hashNextValue = true
              true
            }
          }
        }

        override def next(): InternalRow = {
          val row = if (requiredEmpty) {
            new GenericInternalRow(0)
          } else {
            new GenericInternalRow(len)
          }
          var cell = scanner.getCell
          var lastKey: HBaseRowArrayByteBuff = new HBaseRowArrayByteBuff(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
          var curKey = lastKey
          if (requiredSchemaContainsRowKey) {
            // append row_key to internalRow
            rowKeyConverter(row, rowKeyIdx, cell.getRowArray, cell.getRowOffset, cell.getRowLength)
          }
          while (scanner.isSeeked && lastKey.compareTo(curKey) == 0) {
            lastKey = curKey
            if (!requiredEmpty && !requiredRowKeyOnly) {
              // this hfile already under column family folder, so here family name is unnecessary
              var curQualifierName = new HBaseRowArrayByteBuff(
                cell.getQualifierArray,
                cell.getQualifierOffset,
                cell.getQualifierLength)
              while (!requiredQualifierNameMap.containsKey(curQualifierName) && hashNextValue) {
                if (scanner.next()) {
                  cell = scanner.getCell
                  curQualifierName = new HBaseRowArrayByteBuff(
                    cell.getQualifierArray,
                    cell.getQualifierOffset,
                    cell.getQualifierLength)
                } else {
                  hashNextValue = false
                }
              }
              val (converter, idx) = requiredQualifierNameMap.get(curQualifierName)
              val value = cell.getValueArray
              converter(row, idx, value, cell.getValueOffset, cell.getValueLength)
              // only put needed value into row
            }
            if (scanner.next()) {
              cell = scanner.getCell
              curKey = new HBaseRowArrayByteBuff(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
              hashNextValue = true
            } else {
              hashNextValue = false
            }
          }
          row
        }
      }
    }
  }

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
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
  val rowKeyConverter = HBaseSparkDataUtils.genInternalRowToHBaseConverter(dataSchema(rowKeyIdx).dataType)

  val schemaMap = dataSchema.map { field =>
    val familyQualifierName = field.name.split("_", 2)
    val familyName = Bytes.toBytes(familyQualifierName.head)
    val qualifierName = Bytes.toBytes(familyQualifierName.last)
    field.name -> (
      familyName,
      qualifierName,
      dataSchema.getFieldIndex(field.name),
      HBaseSparkDataUtils.genInternalRowToHBaseConverter(field.dataType))
  }.toMap


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