package org.apache.spark.sql.hbase.utils

import org.apache.hadoop.classification.InterfaceAudience
import org.apache.hadoop.hbase.client.RegionInfo
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil
import org.apache.spark.annotation.{DeveloperApi, Unstable}
import org.apache.spark.util.Utils

import java.io.{ObjectInputStream, ObjectOutputStream}


@DeveloperApi @Unstable
@InterfaceAudience.Private
class SerializableRegionInfo(@transient var regionInfo: RegionInfo) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.defaultWriteObject()
    // 序列化region info
    val regionInfoProtoBytes = RegionInfo.toDelimitedByteArray(regionInfo)
    out.writeInt(regionInfoProtoBytes.length)
    out.write(regionInfoProtoBytes)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    //读取RegionInfo信息
    val len = in.readInt()
    val regionInfoProtoBytes = in.readNBytes(len)
    regionInfo = RegionInfo.parseFrom(regionInfoProtoBytes)
  }

}
