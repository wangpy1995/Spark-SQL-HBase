package org.apache.spark.sql.hbase.client

import java.io.File
import java.lang.reflect.InvocationTargetException
import java.net.{URL, URLClassLoader}
import java.util

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkSubmitUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.NonClosableMutableURLClassLoader
import org.apache.spark.util.{MutableURLClassLoader, Utils}

/**
 * Created by wpy on 2017/5/12.
 */
private[hbase] object IsolatedClientLoader extends Logging {
  /**
   * Creates isolated Hive client loaders by downloading the requested version from maven.
   */
  def forVersion(
                  version: String,
                  hadoopVersion: String,
                  sparkConf: SparkConf,
                  hadoopConf: Configuration,
                  config: Map[String, String] = Map.empty,
                  ivyPath: Option[String] = None,
                  sharedPrefixes: Seq[String] = Seq.empty,
                  barrierPrefixes: Seq[String] = Seq.empty): IsolatedClientLoader = synchronized {
    val resolvedVersion = hbaseVersion(version)
    // We will first try to share Hadoop classes. If we cannot resolve the Hadoop artifact
    // with the given version, we will use Hadoop 2.6 and then will not share Hadoop classes.
    var sharesHadoopClasses = true
    val files = if (resolvedVersions.contains((resolvedVersion, hadoopVersion))) {
      resolvedVersions((resolvedVersion, hadoopVersion))
    } else {
      log.info("downloading jars from maven")
      val (downloadedFiles, actualHadoopVersion) =
        try {
          (downloadVersion(resolvedVersion, hadoopVersion, ivyPath), hadoopVersion)
        } catch {
          case e: RuntimeException if e.getMessage.contains("hadoop") =>
            // If the error message contains hadoop, it is probably because the hadoop
            // version cannot be resolved.
            logWarning(s"Failed to resolve Hadoop artifacts for the version $hadoopVersion. " +
              s"We will change the hadoop version from $hadoopVersion to 2.6.0 and try again. " +
              "Hadoop classes will not be shared between Spark and Hive metastore client. " +
              "It is recommended to set jars used by Hive metastore client through " +
              "spark.sql.hive.metastore.jars in the production environment.")
            sharesHadoopClasses = false
            (downloadVersion(resolvedVersion, "2.6.5", ivyPath), "2.6.5")
        }
      resolvedVersions.put((resolvedVersion, actualHadoopVersion), downloadedFiles)
      resolvedVersions((resolvedVersion, actualHadoopVersion))
    }

    new IsolatedClientLoader(
      hbaseVersion(version),
      sparkConf,
      execJars = files,
      hadoopConf = hadoopConf,
      config = config,
      sharesHadoopClasses = sharesHadoopClasses,
      sharedPrefixes = sharedPrefixes,
      barrierPrefixes = barrierPrefixes)
  }

  def hbaseVersion(version: String): HBaseVersion = version match {
    case "1.0" | "1.0.0" => hbase.v1_0
    case "1.1" | "1.1.0" => hbase.v1_1
    case "1.2" | "1.2.0" | "1.2.1" => hbase.v1_2
    case "2.0.0-SNAPSHOT" | "2.0" | "2.0.0" | "2.0.1" => hbase.v2_0
    case "3.0.0-SNAPSHOT" | "3.0.0-alpha-1-SNAPSHOT" | "3.0.0-alpha-2-SNAPSHOT" | "3.0" | "3.0.0" => hbase.v3_0
  }

  private def downloadVersion(
                               version: HBaseVersion,
                               hadoopVersion: String,
                               ivyPath: Option[String]): Seq[URL] = {
    val hbaseArtifacts = version.extraDeps ++
      Seq("hbase-client", "hbase-common", "hbase-server",
        "hbase-hadoop-compat", "hbase-hadoop2-compat",
        "hbase-metrics", "hbase-metrics-api")
        .map(a => s"org.apache.hbase:$a:${version.fullVersion}") ++
      Seq("com.google.guava:guava:14.0.1",
        s"org.apache.hadoop:hadoop-client:$hadoopVersion")

    val classpath = /*quietly*/ {
      SparkSubmitUtils.resolveMavenCoordinates(
        hbaseArtifacts.mkString(","),
        SparkSubmitUtils.buildIvySettings(
          Some("http://www.datanucleus.org/downloads/maven2"),
          ivyPath),
        exclusions = version.exclusions,
        transitive = true)
    }
    val allFiles = classpath.map(new File(_)).toSet

    // TODO: Remove copy logic.
    val tempDir = Utils.createTempDir(namePrefix = s"hbase-$version")
    allFiles.foreach(f => FileUtils.copyFileToDirectory(f, tempDir))
    logInfo(s"Downloaded metastore jars to ${tempDir.getCanonicalPath}")
    tempDir.listFiles().map(_.toURI.toURL)
  }

  // A map from a given pair of HiveVersion and Hadoop version to jar files.
  // It is only used by forVersion.
  private val resolvedVersions =
  new scala.collection.mutable.HashMap[(HBaseVersion, String), Seq[URL]]
}

private[hbase] class IsolatedClientLoader(
                                           val version: HBaseVersion,
                                           val sparkConf: SparkConf,
                                           val hadoopConf: Configuration,
                                           val execJars: Seq[URL] = Seq.empty,
                                           val config: Map[String, String] = Map.empty,
                                           val isolationOn: Boolean = true,
                                           val sharesHadoopClasses: Boolean = true,
                                           val rootClassLoader: ClassLoader = ClassLoader.getSystemClassLoader.getParent.getParent,
                                           val baseClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader,
                                           val sharedPrefixes: Seq[String] = Seq.empty,
                                           val barrierPrefixes: Seq[String] = Seq.empty)
  extends Logging {

  // Check to make sure that the root classloader does not know about Hive.
  //  assert(Try(rootClassLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf")).isFailure)

  /** All jars used by the hive specific classloader. */
  protected def allJars: Array[URL] = execJars.toArray

  protected def isSharedClass(name: String): Boolean = {
    val isHadoopClass =
      name.startsWith("org.apache.hadoop.") && !name.startsWith("org.apache.hadoop.hbase.")

    name.contains("slf4j") ||
      name.contains("log4j") ||
      name.startsWith("org.apache.spark.") ||
      (sharesHadoopClasses && isHadoopClass) ||
      name.startsWith("scala.") ||
      (name.startsWith("com.google") && !name.startsWith("com.google.cloud")) ||
      name.startsWith("java.lang.") ||
      name.startsWith("java.net") ||
      sharedPrefixes.exists(name.startsWith)
  }

  /** True if `name` refers to a spark class that must see specific version of Hive. */
  protected def isBarrierClass(name: String): Boolean =
    name.startsWith(classOf[HBaseClientImpl].getName) ||
      barrierPrefixes.exists(name.startsWith)

  protected def classToPath(name: String): String =
    name.replaceAll("\\.", "/") + ".class"

  /**
   * The classloader that is used to load an isolated version of Hive.
   * This classloader is a special URLClassLoader that exposes the addURL method.
   * So, when we add jar, we can add this new jar directly through the addURL method
   * instead of stacking a new URLClassLoader on top of it.
   */
  private[hbase] val classLoader: MutableURLClassLoader = {
    val isolatedClassLoader =
      if (isolationOn) {
        new URLClassLoader(allJars, rootClassLoader) {
          override def loadClass(name: String, resolve: Boolean): Class[_] = {
            val loaded = findLoadedClass(name)
            if (loaded == null) doLoadClass(name, resolve) else loaded
          }

          def doLoadClass(name: String, resolve: Boolean): Class[_] = {
            val classFileName = name.replaceAll("\\.", "/") + ".class"
            if (isBarrierClass(name)) {
              // For barrier classes, we construct a new copy of the class.
              val bytes = IOUtils.toByteArray(baseClassLoader.getResourceAsStream(classFileName))
              logDebug(s"custom defining: $name - ${util.Arrays.hashCode(bytes)}")
              defineClass(name, bytes, 0, bytes.length)
            } else if (!isSharedClass(name)) {
              logDebug(s"hbase class: $name - ${getResource(classToPath(name))}")
              super.loadClass(name, resolve)
            } else {
              // For shared classes, we delegate to baseClassLoader, but fall back in case the
              // class is not found.
              logDebug(s"shared class: $name")
              try {
                baseClassLoader.loadClass(name)
              } catch {
                case _: ClassNotFoundException =>
                  super.loadClass(name, resolve)
              }
            }
          }
        }
      } else {
        baseClassLoader
      }
    // Right now, we create a URLClassLoader that gives preference to isolatedClassLoader
    // over its own URLs when it loads classes and resources.
    // We may want to use ChildFirstURLClassLoader based on
    // the configuration of spark.executor.userClassPathFirst, which gives preference
    // to its own URLs over the parent class loader (see Executor's createClassLoader method).
    new NonClosableMutableURLClassLoader(isolatedClassLoader)
  }

  private[hbase] def addJar(path: URL): Unit = synchronized {
    classLoader.addURL(path)
  }

  /** The isolated client interface to Hive. */
  private[hbase] def createClient(): HBaseClient = {
    if (!isolationOn) {
      return new HBaseClientImpl(version, sparkConf, hadoopConf, config, baseClassLoader, this)
    }
    // Pre-reflective instantiation setup.
    logDebug("Initializing the logger to avoid disaster...")
    val origLoader = Thread.currentThread().getContextClassLoader
    Thread.currentThread.setContextClassLoader(classLoader)

    try {
      classLoader
        .loadClass(classOf[HBaseClientImpl].getName)
        .getConstructors.head
        .newInstance(version, sparkConf, hadoopConf, config, classLoader, this)
        .asInstanceOf[HBaseClient]
    } catch {
      case e: InvocationTargetException =>
        e.getCause match {
          case cnf: NoClassDefFoundError =>
            throw new ClassNotFoundException(
              s"$cnf when creating HBase client using classpath: ${execJars.mkString(", ")}\n" +
                "Please make sure that jars for your version of hive and hadoop are included in the " +
                s"paths passed to ${HConstants.VERSION_FILE_NAME}.", e)
          case _ =>
            throw e
        }
    } finally {
      Thread.currentThread.setContextClassLoader(origLoader)
    }
  }

  /**
   * The place holder for shared Hive client for all the HiveContext sessions (they share an
   * IsolatedClientLoader).
   */
  private[hbase] var cachedConnection: Any = _

  private[hbase] var cachedAdmin: Any = _
}
