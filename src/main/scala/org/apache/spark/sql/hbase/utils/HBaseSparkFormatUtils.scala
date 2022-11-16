package org.apache.spark.sql.hbase.utils

import org.apache.spark.sql.hbase.SparkHBaseConstants.TABLE_CONSTANTS

object HBaseSparkFormatUtils {
  case class SeparateName(familyName: String, qualifierName: String)

  def splitColumnAndQualifierName(columnQualifierNameString: String): SeparateName = {
    val results = columnQualifierNameString.split(TABLE_CONSTANTS.COLUMN_QUALIFIER_SPLITTER.getValue, 2)
    SeparateName(results.head, results.last)
  }

  def combineColumnAndQualifierName(familyName: String, qualifierName: String): String = {
    familyName + TABLE_CONSTANTS.COLUMN_QUALIFIER_SPLITTER.getValue + qualifierName
  }
}
