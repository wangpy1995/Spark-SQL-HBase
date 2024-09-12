package org.apache.spark.sql.hbase

/**
 * Created by wpy on 2017/5/11.
 */
//TODO add shim loader in the future
package object client {

  //  val jruby = new ScriptingContainer()

  /** 兼容多版本 */
  private[client] sealed abstract class HBaseVersion(
                                                     val fullVersion: String,
                                                     val extraDeps: Seq[String] = Nil,
                                                     val exclusions: Seq[String] = Nil)

  private[client] object hbase {

    case object v1_0 extends HBaseVersion("1.0.0",
      exclusions = Seq("jdk.tools:jdk.tools",
        "org.glassfish.hk2:*",
        "org.glassfish.jersey.bundles.repackaged:jersey-guava",
        "org.glassfish.hk2.external:javax.inject"))

    case object v1_1 extends HBaseVersion("1.1.0",
      exclusions = Seq("jdk.tools:jdk.tools",
        "org.glassfish.hk2:*",
        "org.glassfish.jersey.bundles.repackaged:jersey-guava",
        "org.glassfish.hk2.external:javax.inject"))

    case object v1_2 extends HBaseVersion("1.2.0",
      exclusions = Seq("jdk.tools:jdk.tools",
        "org.glassfish.hk2:*",
        "org.glassfish.jersey.bundles.repackaged:jersey-guava",
        "org.glassfish.hk2.external:javax.inject"))

    case object v2_0 extends HBaseVersion("2.0.0-SNAPSHOT",
      exclusions = Seq("jdk.tools:jdk.tools",
        "org.glassfish.hk2:*",
        "org.glassfish.jersey.bundles.repackaged:jersey-guava",
        "org.glassfish.hk2.external:javax.inject"))

    case object v3_0 extends HBaseVersion("3.0.0-alpha-4-SNAPSHOT",
      exclusions = Seq("jdk.tools:jdk.tools",
        "org.glassfish.hk2:*",
        "org.glassfish.jersey.bundles.repackaged:jersey-guava",
        "org.glassfish.hk2.external:javax.inject"))

    val allSupportedHBaseVersions: Set[HBaseVersion] = Set(v1_0, v1_1, v1_2, v2_0, v3_0)
  }

}
