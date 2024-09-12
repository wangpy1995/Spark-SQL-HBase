package org.apache.spark.sql.hbase.utils

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.hbase.utils.StructFieldConverters.{fromAttribute, toAttribute}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * 用于转换StructType到Attribute
 */
object StructFieldConverters {

  implicit def toAttribute(field: StructField): AsAttribute = new AsAttribute(field)

  implicit def toAttributes(structType: StructType): AsAttributes =new AsAttributes(structType)



  implicit def fromAttribute(attribute: AttributeReference): AsStructField = new AsStructField(attribute)



  implicit def fromAttributes(attributes: Seq[AttributeReference]): AsStructType = new AsStructType(attributes)
}

final class AsAttribute(value:StructField){
  def toAttribute: AttributeReference = AttributeReference(value.name, value.dataType, value.nullable, value.metadata)()
}

final class AsAttributes(value:StructType){
  def toAttributes: Seq[AttributeReference] = value.map(_.toAttribute)
}

final class AsStructField(reference: AttributeReference){
  def fromAttribute:StructField = StructField(reference.name,reference.dataType,reference.nullable,reference.metadata)
}

final class AsStructType(attributes:Seq[AttributeReference]){
  def fromAttributes:StructType = StructType(attributes.map(attr=>attr.fromAttribute))
}