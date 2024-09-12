package org.apache.spark.sql.hbase

import jline.console.ConsoleReader
import jline.console.history.FileHistory
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.File
import java.util.Properties

/**
  * Created by wpy on 17-5-18.
  */
object HBaseSQLClient {
  val prompt = "HBaseSQL "
  import scala.collection.JavaConverters._
  private val extraConfigs = {
    val in = getClass.getResourceAsStream("/spark_hbase.properties")
    val props = new Properties()
    props.load(in)
    in.close()
    props.asScala.toMap
  }
  private val continuedPrompt = "".padTo(prompt.length, ' ')
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.hadoopRDD.ignoreEmptySplits","false")
  private val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  private val sc = ss.sparkContext
  val hs = new HBaseSession(sc, new Configuration(),extraConfigs)

  def main(args: Array[String]): Unit = {

    val reader = new ConsoleReader()
    reader.setBellEnabled(false)
    val historyDirectory = System.getProperty("user.home")

    try {
      if (new File(historyDirectory).exists()) {
        val historyFile = historyDirectory + File.separator + ".hbaseqlhistory"
        reader.setHistory(new FileHistory(new File(historyFile)))
      }

    } catch {
      case e: Exception =>
        System.err.println(e.getMessage)
    }

    println("Spark4HBase CLI")
    var prefix = ""

    def promptPrefix = {
      s"$prompt"
    }

    var currentPrompt = promptPrefix
    var line = reader.readLine(currentPrompt + ": $ ")

    while (line != null) {
      if (prefix.nonEmpty) {
        prefix += '\n'
      }

      if (line.trim.endsWith(";") && !line.trim.endsWith("\\;")) {
        line = prefix + line
        processLine(line, allowInterrupting = true)
        prefix = ""
        currentPrompt = promptPrefix
      }
      else {
        prefix = prefix + line
        currentPrompt = continuedPrompt
      }

      line = reader.readLine(currentPrompt + " $ ")
    }

    System.exit(0)
  }


  private def processLine(line: String, allowInterrupting: Boolean): Unit = {

    val input = line.substring(0, line.length - 1)
    try {
      process(input.trim())
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  private def process(input: String): Unit = {
    val token = input.split("\\s")
    token(0).toUpperCase match {
      case "EXIT" => ss.close()
        System.exit(0)

      case _ => hs.sql(input).show(30)
    }
  }

}
