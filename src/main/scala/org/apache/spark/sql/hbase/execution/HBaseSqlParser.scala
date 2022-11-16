package org.apache.spark.sql.hbase.execution

import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.PrimitiveDataTypeContext
import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, SqlBaseParser}
import org.apache.spark.sql.errors.QueryParsingErrors
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.hbase.types.RegionInfoUDT
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.types._

import java.util.Locale
import scala.collection.JavaConverters._

class HBaseSqlParser extends AbstractSqlParser {
  val astBuilder = new SparkHBaseSqlAstBuilder()

  private val substitutor = new VariableSubstitution()

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }
}

object HBaseSqlParser extends HBaseSqlParser

class SparkHBaseSqlAstBuilder extends SparkSqlAstBuilder {
  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = withOrigin(ctx) {
    val dataType = ctx.identifier.getText.toLowerCase(Locale.ROOT)
    (dataType, ctx.INTEGER_VALUE().asScala.toList) match {
      case ("boolean", Nil) => BooleanType
      case ("tinyint" | "byte", Nil) => ByteType
      case ("smallint" | "short", Nil) => ShortType
      case ("int" | "integer", Nil) => IntegerType
      case ("bigint" | "long", Nil) => LongType
      case ("float" | "real", Nil) => FloatType
      case ("double", Nil) => DoubleType
      case ("date", Nil) => DateType
      case ("timestamp", Nil) => SQLConf.get.timestampType
      case ("timestamp_ntz", Nil) => TimestampNTZType
      case ("timestamp_ltz", Nil) => TimestampType
      case ("string", Nil) => StringType
      case ("character" | "char", length :: Nil) => CharType(length.getText.toInt)
      case ("varchar", length :: Nil) => VarcharType(length.getText.toInt)
      case ("binary", Nil) => BinaryType
      case ("decimal" | "dec" | "numeric", Nil) => DecimalType.USER_DEFAULT
      case ("decimal" | "dec" | "numeric", precision :: Nil) =>
        DecimalType(precision.getText.toInt, 0)
      case ("decimal" | "dec" | "numeric", precision :: scale :: Nil) =>
        DecimalType(precision.getText.toInt, scale.getText.toInt)
      case ("void", Nil) => NullType
      case ("interval", Nil) => CalendarIntervalType
      case ("regioninfo", Nil) => RegionInfoUDT
      case (dt@("character" | "char" | "varchar"), Nil) =>
        throw QueryParsingErrors.charTypeMissingLengthError(dt, ctx)
      case (dt, params) =>
        val dtStr = if (params.nonEmpty) s"$dt(${params.mkString(",")})" else dt
        throw QueryParsingErrors.dataTypeUnsupportedError(dtStr, ctx)
    }
  }
}