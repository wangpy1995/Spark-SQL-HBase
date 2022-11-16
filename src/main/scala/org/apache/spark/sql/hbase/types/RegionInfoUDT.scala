package org.apache.spark.sql.hbase.types

import org.apache.hadoop.hbase.client.RegionInfo
import org.apache.spark.sql.types.{BinaryType, DataType, ObjectType, UserDefinedType}

class RegionInfoUDT extends UserDefinedType[RegionInfo] {

  override def sqlType: DataType = BinaryType

  override def serialize(obj: RegionInfo): Array[Byte] = RegionInfo.toDelimitedByteArray(obj)

  override def deserialize(datum: Any): RegionInfo = datum match {
    case pb: Array[Byte] => RegionInfo.parseFrom(pb)
    case _ => null
  }

  override def userClass: Class[RegionInfo] = classOf[RegionInfo]
}

case object RegionInfoUDT extends RegionInfoUDT
