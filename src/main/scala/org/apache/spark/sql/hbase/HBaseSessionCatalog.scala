package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.internal.SQLConf

/**
  * Created by wpy on 17-5-17.
  */
private[sql] class HBaseSessionCatalog(
                                        externalCatalog: HBaseExternalCatalog,
                                        globalTempViewManager: GlobalTempViewManager,
                                        functionRegistry: FunctionRegistry,
                                        conf: SQLConf,
                                        hadoopConf: Configuration,
                                        parser: ParserInterface,
                                        functionResourceLoader: FunctionResourceLoader)
  extends SessionCatalog(
    externalCatalog,
    globalTempViewManager,
    functionRegistry,
    conf,
    hadoopConf,
    parser,
    functionResourceLoader) {

}
